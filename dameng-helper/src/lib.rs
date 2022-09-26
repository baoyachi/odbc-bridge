#[allow(non_camel_case_types)]
pub mod data_type;
pub mod error;

use crate::error::DmError;
use anyhow::anyhow;
pub use data_type::*;
use odbc_api::buffers::TextRowSet;
use odbc_api::handles::StatementImpl;
use odbc_api::{ColumnDescription, Cursor, CursorImpl, Error, ResultSetMetadata};
use std::str::FromStr;

#[derive(Debug, Default)]
pub struct DmColumnDesc {
    table_id: Option<usize>,
    inner: Vec<DmColumnInner>,
}

#[derive(Debug)]
pub struct DmColumnInner {
    name: String,
    data_type: DataType,
}

impl DmColumnInner {
    fn new(name: String, data_type: DataType) -> Self {
        Self { name, data_type }
    }
}

pub trait DmAdapter {
    fn get_table_sql(table_name: &str) -> String;
    fn get_table_desc(self) -> anyhow::Result<DmColumnDesc>;
}

impl DmAdapter for CursorImpl<StatementImpl<'_>> {
    fn get_table_sql(table_name: &str) -> String {
        // use sql: select a.name,a.type$ as data_type,a.id as table_id from SYSCOLUMNS as a left join SYSOBJECTS as b on a.id = b.id where b.name = '?'
        format!(
            r#"select a.name,a.type$ as data_type,a.id as table_id from SYSCOLUMNS as a left join SYSOBJECTS as b on a.id = b.id where b.name = '{}'"#,
            table_name
        )
    }

    fn get_table_desc(mut self) -> anyhow::Result<DmColumnDesc> {
        let headers = self.column_names()?.collect::<Result<Vec<String>, _>>()?;

        assert_eq!(headers, vec!["name", "data_type", "table_id"]);

        let mut buffers = TextRowSet::for_cursor(1024, &mut self, Some(4096))?;
        let mut row_set_cursor = self.bind_buffer(&mut buffers)?;
        let mut table_desc = DmColumnDesc::default();

        while let Some(batch) = row_set_cursor.fetch()? {
            for row_index in 0..batch.num_rows() {
                let num_cols = batch.num_cols();
                assert_eq!(num_cols, headers.len());

                let mut row_data: Vec<_> = (0..num_cols)
                    .map(|col_index| batch.at(col_index, row_index).unwrap_or(&[]))
                    .into_iter()
                    .map(|x| String::from_utf8_lossy(x))
                    .collect();
                table_desc.inner.push(DmColumnInner::new(
                    row_data.remove(0).to_string(),
                    DataType::from_str(row_data.remove(0).as_ref())?,
                ));
                if table_desc.table_id.is_none() {
                    table_desc.table_id = Some(row_data.remove(0).parse::<usize>()?);
                }
            }
        }
        Ok(table_desc)
    }
}
