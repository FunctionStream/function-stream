//! 维表 Lookup Join（Enrichment）：与 worker `arrow/lookup_join` 逻辑对齐，实现 [`MessageOperator`]。

use anyhow::{anyhow, Result};
use arrow::compute::filter_record_batch;
use arrow::row::{OwnedRow, RowConverter, SortField};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use async_trait::async_trait;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use mini_moka::sync::Cache;
use prost::Message;
use protocol::grpc::api::{JoinType, LookupJoinOperator as LookupJoinProto};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::{MessageOperator, Registry};
use crate::runtime::streaming::connectors::{LookupConnector, connectors};
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::{CheckpointBarrier, FsSchema, MetadataField, OperatorConfig, Watermark, LOOKUP_KEY_INDEX_FIELD};

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum LookupJoinType {
    Left,
    Inner,
}

/// 维表查询连接算子：外部系统打宽 + 可选 LRU 缓存。
pub struct LookupJoinOperator {
    name: String,
    connector: Box<dyn LookupConnector + Send>,
    key_exprs: Vec<Arc<dyn PhysicalExpr>>,
    cache: Option<Cache<OwnedRow, OwnedRow>>,
    key_row_converter: RowConverter,
    result_row_converter: RowConverter,
    join_type: LookupJoinType,
    lookup_schema: Arc<Schema>,
    metadata_fields: Vec<MetadataField>,
    input_schema: Arc<FsSchema>,
    /// 与 worker 侧 `ctx.out_schema` 对齐：由 input 去 key + lookup 列 + 时间列拼成。
    output_schema: Arc<Schema>,
}

fn build_lookup_output_schema(
    input: &FsSchema,
    lookup_columns: &[FieldRef],
) -> anyhow::Result<Arc<Schema>> {
    let key_indices = input.routing_keys().cloned().unwrap_or_default();
    let ts = input.timestamp_index;
    let mut out: Vec<FieldRef> = Vec::new();
    for i in 0..input.schema.fields().len() {
        if key_indices.contains(&i) || i == ts {
            continue;
        }
        out.push(input.schema.fields()[i].clone());
    }
    out.extend(lookup_columns.iter().cloned());
    out.push(input.schema.fields()[ts].clone());
    Ok(Arc::new(Schema::new(out)))
}

impl LookupJoinOperator {
    async fn process_lookup_batch(&mut self, batch: RecordBatch) -> Result<Vec<StreamOutput>> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(vec![]);
        }

        let key_arrays: Vec<_> = self
            .key_exprs
            .iter()
            .map(|expr| {
                expr.evaluate(&batch)
                    .map_err(|e| anyhow!("key expr evaluate: {e}"))?
                    .into_array(num_rows)
                    .map_err(|e| anyhow!("key expr into_array: {e}"))
            })
            .collect::<Result<_>>()?;

        let rows = self
            .key_row_converter
            .convert_columns(&key_arrays)
            .map_err(|e| anyhow!("key_row_converter: {e}"))?;

        let mut key_map: HashMap<OwnedRow, Vec<usize>> = HashMap::new();
        for (i, row) in rows.iter().enumerate() {
            key_map.entry(row.owned()).or_default().push(i);
        }

        let uncached_keys: Vec<&OwnedRow> = if let Some(cache) = &mut self.cache {
            key_map
                .keys()
                .filter(|k| !cache.contains_key(*k))
                .collect()
        } else {
            key_map.keys().collect()
        };

        // 按 key 字节存 OwnedRow，避免借用 `convert_columns` 返回的临时行缓冲。
        let mut results: HashMap<Vec<u8>, OwnedRow> = HashMap::new();

        if !uncached_keys.is_empty() {
            let cols = self
                .key_row_converter
                .convert_rows(uncached_keys.iter().map(|r| r.row()))
                .map_err(|e| anyhow!("convert_rows for lookup: {e}"))?;

            if let Some(result_batch) = self.connector.lookup(&cols).await {
                let mut result_batch = result_batch.map_err(|e| anyhow!("connector lookup: {e}"))?;

                let key_idx_col = result_batch
                    .schema()
                    .index_of(LOOKUP_KEY_INDEX_FIELD)
                    .map_err(|e| anyhow!("{e}"))?;
                let keys = result_batch.remove_column(key_idx_col);
                let keys = keys.as_primitive::<UInt64Type>();

                let result_rows = self
                    .result_row_converter
                    .convert_columns(result_batch.columns())
                    .map_err(|e| anyhow!("result_row_converter: {e}"))?;

                for (i, v) in result_rows.iter().enumerate() {
                    if keys.is_null(i) {
                        return Err(anyhow!("lookup key index is null at row {i}"));
                    }
                    let req_idx = keys.value(i) as usize;
                    if req_idx >= uncached_keys.len() {
                        return Err(anyhow!(
                            "lookup key index {req_idx} out of range ({} keys)",
                            uncached_keys.len()
                        ));
                    }
                    let key_bytes = uncached_keys[req_idx].as_ref().to_vec();
                    let owned = v.owned();
                    results.insert(key_bytes.clone(), owned.clone());
                    if let Some(cache) = &mut self.cache {
                        cache.insert(uncached_keys[req_idx].clone(), owned);
                    }
                }
            }
        }

        let mut output_rows = self
            .result_row_converter
            .empty_rows(batch.num_rows(), batch.num_rows().saturating_mul(10));

        for row in rows.iter() {
            let row_owned = self
                .cache
                .as_mut()
                .and_then(|c| c.get(&row.owned()))
                .unwrap_or_else(|| {
                    results
                        .get(row.as_ref())
                        .expect("missing lookup result for key (cache miss without connector row)")
                        .clone()
                });
            output_rows.push(row_owned.row());
        }

        let right_side = self
            .result_row_converter
            .convert_rows(output_rows.iter())
            .map_err(|e| anyhow!("convert_rows output: {e}"))?;

        let nonnull = (self.join_type == LookupJoinType::Inner).then(|| {
            let mut nonnull = vec![false; batch.num_rows()];
            for (_, a) in self
                .lookup_schema
                .fields()
                .iter()
                .zip(right_side.iter())
                .filter(|(f, _)| {
                    !self
                        .metadata_fields
                        .iter()
                        .any(|m| &m.field_name == f.name())
                })
            {
                if let Some(nulls) = a.logical_nulls() {
                    for (valid, b) in nulls.iter().zip(nonnull.iter_mut()) {
                        *b |= valid;
                    }
                } else {
                    nonnull.fill(true);
                    break;
                }
            }
            BooleanArray::from(nonnull)
        });

        let key_indices = self
            .input_schema
            .routing_keys()
            .cloned()
            .unwrap_or_default();
        let non_keys: Vec<_> = (0..batch.num_columns())
            .filter(|i| !key_indices.contains(i) && *i != self.input_schema.timestamp_index)
            .collect();

        let mut result_cols = batch
            .project(&non_keys)
            .map_err(|e| anyhow!("project non_keys: {e}"))?
            .columns()
            .to_vec();
        result_cols.extend(right_side);
        result_cols.push(batch.column(self.input_schema.timestamp_index).clone());

        let mut out_batch = RecordBatch::try_new(self.output_schema.clone(), result_cols)
            .map_err(|e| anyhow!("try_new output batch: {e}"))?;

        if let Some(mask) = nonnull {
            out_batch = filter_record_batch(&out_batch, &mask).map_err(|e| anyhow!("{e}"))?;
        }

        if out_batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        Ok(vec![StreamOutput::Forward(out_batch)])
    }
}

#[async_trait]
impl MessageOperator for LookupJoinOperator {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        Ok(())
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        self.process_lookup_batch(batch).await
    }

    async fn process_watermark(
        &mut self,
        _watermark: Watermark,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }

    async fn snapshot_state(
        &mut self,
        _barrier: CheckpointBarrier,
        _ctx: &mut TaskContext,
    ) -> Result<()> {
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}

/// 从配置构造 [`LookupJoinOperator`]（非 `ConstructedOperator` / `ArrowOperator`）。
pub struct LookupJoinConstructor;

impl LookupJoinConstructor {
    pub fn with_config(
        &self,
        config: LookupJoinProto,
        registry: Arc<Registry>,
    ) -> anyhow::Result<LookupJoinOperator> {
        let join_type = config.join_type();
        let input_schema: FsSchema = config.input_schema.unwrap().try_into()?;
        let lookup_schema: FsSchema = config.lookup_schema.unwrap().try_into()?;

        let exprs = config
            .key_exprs
            .iter()
            .map(|e| {
                let expr = PhysicalExprNode::decode(&mut e.left_expr.as_slice())?;
                Ok(parse_physical_expr(
                    &expr,
                    registry.as_ref(),
                    &input_schema.schema,
                    &DefaultPhysicalExtensionCodec {},
                )?)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let op = config.connector.unwrap();
        let operator_config: OperatorConfig = serde_json::from_str(&op.config)?;

        let result_row_converter = RowConverter::new(
            lookup_schema
                .schema_without_timestamp()
                .fields
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        let lookup_schema_arc = Arc::new(
            lookup_schema
                .with_additional_fields(
                    [Field::new(LOOKUP_KEY_INDEX_FIELD, DataType::UInt64, false)].into_iter(),
                )?
                .schema_without_timestamp(),
        );

        let output_schema = build_lookup_output_schema(&input_schema, lookup_schema_arc.fields())?;

        let connector = connectors()
            .get(op.connector.as_str())
            .unwrap_or_else(|| panic!("No connector with name '{}'", op.connector))
            .make_lookup(operator_config.clone(), lookup_schema_arc.clone())?;

        let name = format!("LookupJoin({})", connector.name());

        let max_capacity_bytes = config.max_capacity_bytes.unwrap_or(8 * 1024 * 1024);
        let cache = (max_capacity_bytes > 0).then(|| {
            let mut c = Cache::builder()
                .weigher(|k: &OwnedRow, v: &OwnedRow| (k.as_ref().len() + v.as_ref().len()) as u32)
                .max_capacity(max_capacity_bytes);

            if let Some(ttl) = config.ttl_micros {
                c = c.time_to_live(Duration::from_micros(ttl));
            }
            c.build()
        });

        let key_row_converter = RowConverter::new(
            exprs
                .iter()
                .map(|e| Ok(SortField::new(e.data_type(&input_schema.schema)?)))
                .collect::<anyhow::Result<_>>()?,
        )?;

        Ok(LookupJoinOperator {
            name,
            connector,
            key_exprs: exprs,
            cache,
            key_row_converter,
            result_row_converter,
            join_type: match join_type {
                JoinType::Inner => LookupJoinType::Inner,
                JoinType::Left => LookupJoinType::Left,
                jt => panic!("invalid lookup join type {:?}", jt),
            },
            lookup_schema: lookup_schema_arc,
            metadata_fields: operator_config.metadata_fields,
            input_schema: Arc::new(input_schema),
            output_schema,
        })
    }
}

