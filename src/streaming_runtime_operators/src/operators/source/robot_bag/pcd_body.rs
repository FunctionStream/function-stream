// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{Context as _, Result, bail};
use byteorder::{ByteOrder, LittleEndian};
use serde_json::{Map, Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PcdDataMode {
    Ascii,
    Binary,
    BinaryCompressed,
}

#[derive(Debug, Clone)]
pub struct PcdHeader {
    pub fields: Vec<String>,
    pub sizes: Vec<u32>,
    pub types: Vec<char>,
    pub counts: Vec<u32>,
    pub width: u32,
    pub height: u32,
    pub viewpoint: [f64; 7],
    pub points: usize,
    pub data_mode: PcdDataMode,
    pub point_step: usize,
    pub field_offsets: Vec<usize>,
}

pub(crate) fn find_data_body_offset(data: &[u8]) -> Result<usize> {
    let mut offset = 0usize;
    while offset < data.len() {
        let line_start = offset;
        let line_end = data[offset..]
            .iter()
            .position(|&b| b == b'\n')
            .map(|pos| line_start + pos)
            .unwrap_or(data.len());
        let line_bytes = &data[line_start..line_end];
        let line = std::str::from_utf8(line_bytes).context("pcd header line must be utf-8")?;
        let trimmed = line.trim();
        if !trimmed.is_empty()
            && !trimmed.starts_with('#')
            && trimmed.to_ascii_uppercase().starts_with("DATA ")
        {
            return Ok(if line_end < data.len() {
                line_end + 1
            } else {
                line_end
            });
        }
        if line_end >= data.len() {
            break;
        }
        offset = line_end + 1;
    }
    bail!("PCD header missing DATA line")
}

pub(crate) fn decompress_binary_compressed_body(body: &[u8], header: &PcdHeader) -> Result<Vec<u8>> {
    if body.len() < 8 {
        bail!("binary_compressed body too short");
    }
    let compressed_size = LittleEndian::read_u32(&body[0..4]) as usize;
    let uncompressed_size = LittleEndian::read_u32(&body[4..8]) as usize;
    if body.len() < 8 + compressed_size {
        bail!(
            "binary_compressed truncated: need {} bytes, got {}",
            8 + compressed_size,
            body.len()
        );
    }
    let compressed = &body[8..8 + compressed_size];
    let decompressed = lzf::decompress(compressed, uncompressed_size)
        .map_err(|e| anyhow::anyhow!("LZF decompress failed: {e:?}"))?;
    if decompressed.len() != uncompressed_size {
        bail!(
            "LZF decompressed size mismatch: expected {uncompressed_size}, got {}",
            decompressed.len()
        );
    }

    let expected: usize = header
        .sizes
        .iter()
        .zip(&header.counts)
        .map(|(&s, &c)| header.points * (s * c) as usize)
        .sum();
    if decompressed.len() != expected {
        bail!(
            "binary_compressed uncompressed payload size mismatch: expected {expected}, got {}",
            decompressed.len()
        );
    }

    let mut aos = vec![0u8; header.points * header.point_step];
    for (field_idx, (&size, &count)) in header.sizes.iter().zip(&header.counts).enumerate() {
        let field_bytes = (size * count) as usize;
        let soa_offset: usize = header
            .sizes
            .iter()
            .zip(&header.counts)
            .take(field_idx)
            .map(|(&s, &c)| header.points * (s * c) as usize)
            .sum();
        let point_offset = header.field_offsets[field_idx];
        for pt in 0..header.points {
            let src = soa_offset + pt * field_bytes;
            let dst = pt * header.point_step + point_offset;
            aos[dst..dst + field_bytes].copy_from_slice(&decompressed[src..src + field_bytes]);
        }
    }
    Ok(aos)
}

pub(crate) fn parse_ascii_rows(body: &str, header: &PcdHeader) -> Result<Vec<Vec<f64>>> {
    let values_per_point: usize = header.counts.iter().map(|c| *c as usize).sum();
    let mut rows = Vec::with_capacity(header.points);
    let mut tokens = body.split_whitespace();
    for pt in 0..header.points {
        let mut row = Vec::with_capacity(values_per_point);
        for _ in 0..values_per_point {
            let Some(tok) = tokens.next() else {
                bail!("PCD ascii data ended early at point {pt}");
            };
            row.push(tok.parse::<f64>().with_context(|| format!("ascii token '{tok}'"))?);
        }
        rows.push(row);
    }
    Ok(rows)
}

pub(crate) fn fill_point_object_from_flat_values(
    header: &PcdHeader,
    values: &[f64],
    obj: &mut Map<String, Value>,
) -> Result<()> {
    let mut cursor = 0;
    for (field_idx, field_name) in header.fields.iter().enumerate() {
        let count = header.counts[field_idx] as usize;
        if cursor + count > values.len() {
            bail!("PCD ascii row too short for field '{field_name}'");
        }
        insert_field_values(obj, field_name, &values[cursor..cursor + count]);
        cursor += count;
    }
    Ok(())
}

pub(crate) fn append_binary_point_fields(
    body: &[u8],
    header: &PcdHeader,
    point_index: usize,
    obj: &mut Map<String, Value>,
) -> Result<()> {
    let start = point_index * header.point_step;
    let end = start + header.point_step;
    if end > body.len() {
        bail!(
            "PCD binary data too short for point {point_index}: need {end}, got {}",
            body.len()
        );
    }
    fill_point_object_from_bytes(header, &body[start..end], obj)
}

fn fill_point_object_from_bytes(
    header: &PcdHeader,
    point_bytes: &[u8],
    obj: &mut Map<String, Value>,
) -> Result<()> {
    for (field_idx, field_name) in header.fields.iter().enumerate() {
        let offset = header.field_offsets[field_idx];
        let size = header.sizes[field_idx] as usize;
        let count = header.counts[field_idx] as usize;
        let mut values = Vec::with_capacity(count);
        for elem in 0..count {
            let start = offset + elem * size;
            let end = start + size;
            if end > point_bytes.len() {
                bail!("PCD binary point truncated at field '{field_name}'");
            }
            values.push(decode_field_value(
                header.types[field_idx],
                size,
                &point_bytes[start..end],
            )?);
        }
        insert_field_values(obj, field_name, &values);
    }
    Ok(())
}

pub(crate) fn decode_field_value(ty: char, size: usize, bytes: &[u8]) -> Result<f64> {
    let v = match (ty, size) {
        ('I', 1) => bytes[0] as i8 as f64,
        ('I', 2) => LittleEndian::read_i16(bytes) as f64,
        ('I', 4) => LittleEndian::read_i32(bytes) as f64,
        ('U', 1) => bytes[0] as f64,
        ('U', 2) => LittleEndian::read_u16(bytes) as f64,
        ('U', 4) => LittleEndian::read_u32(bytes) as f64,
        ('F', 4) => LittleEndian::read_f32(bytes) as f64,
        ('F', 8) => LittleEndian::read_f64(bytes),
        _ => bail!("unsupported PCD field TYPE '{ty}' SIZE {size}"),
    };
    Ok(v)
}

pub(crate) fn insert_field_values(obj: &mut Map<String, Value>, field_name: &str, values: &[f64]) {
    match values.len() {
        0 => {}
        1 => {
            obj.insert(field_name.to_string(), json!(values[0]));
        }
        _ => {
            for (i, v) in values.iter().enumerate() {
                obj.insert(format!("{field_name}_{i}"), json!(v));
            }
        }
    }
}
