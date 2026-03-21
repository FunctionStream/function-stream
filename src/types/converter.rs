use std::sync::Arc;
use arrow::row::{OwnedRow, RowConverter, RowParser, Rows, SortField};
use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_schema::{ArrowError, DataType};

// need to handle the empty case as a row converter without sort fields emits empty Rows.
#[derive(Debug)]
pub enum Converter {
    RowConverter(RowConverter),
    Empty(RowConverter, Arc<dyn Array>),
}

impl Converter {
    pub fn new(sort_fields: Vec<SortField>) -> Result<Self, ArrowError> {
        if sort_fields.is_empty() {
            let array = Arc::new(BooleanArray::from(vec![false]));
            Ok(Self::Empty(
                RowConverter::new(vec![SortField::new(DataType::Boolean)])?,
                array,
            ))
        } else {
            Ok(Self::RowConverter(RowConverter::new(sort_fields)?))
        }
    }

    pub fn convert_columns(&self, columns: &[Arc<dyn Array>]) -> Result<OwnedRow, ArrowError> {
        match self {
            Converter::RowConverter(row_converter) => {
                Ok(row_converter.convert_columns(columns)?.row(0).owned())
            }
            Converter::Empty(row_converter, array) => Ok(row_converter
                .convert_columns(std::slice::from_ref(array))?
                .row(0)
                .owned()),
        }
    }

    pub fn convert_all_columns(
        &self,
        columns: &[Arc<dyn Array>],
        num_rows: usize,
    ) -> Result<Rows, ArrowError> {
        match self {
            Converter::RowConverter(row_converter) => Ok(row_converter.convert_columns(columns)?),
            Converter::Empty(row_converter, _array) => {
                let array = Arc::new(BooleanArray::from(vec![false; num_rows]));
                Ok(row_converter.convert_columns(&[array])?)
            }
        }
    }

    pub fn convert_rows(
        &self,
        rows: Vec<arrow::row::Row<'_>>,
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        match self {
            Converter::RowConverter(row_converter) => Ok(row_converter.convert_rows(rows)?),
            Converter::Empty(_row_converter, _array) => Ok(vec![]),
        }
    }

    pub fn convert_raw_rows(&self, row_bytes: Vec<&[u8]>) -> Result<Vec<ArrayRef>, ArrowError> {
        match self {
            Converter::RowConverter(row_converter) => {
                let parser = row_converter.parser();
                let mut row_list = vec![];
                for bytes in row_bytes {
                    let row = parser.parse(bytes);
                    row_list.push(row);
                }
                Ok(row_converter.convert_rows(row_list)?)
            }
            Converter::Empty(_row_converter, _array) => Ok(vec![]),
        }
    }

    pub fn parser(&self) -> Option<RowParser> {
        match self {
            Converter::RowConverter(r) => Some(r.parser()),
            Converter::Empty(_, _) => None,
        }
    }
}