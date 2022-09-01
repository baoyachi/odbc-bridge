use bytes::Bytes;
use odbc_api::DataType;

pub struct ExecResult {
    pub rows_affected: u64,
}

#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub data: Vec<Vec<Bytes>>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum ValueFormat {
    Text = 0,
    Binary = 1,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub type_info: DataType,
    pub format: ValueFormat,
}
