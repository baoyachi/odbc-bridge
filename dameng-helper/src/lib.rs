#![deny(missing_debug_implementations)]

#[allow(non_camel_case_types)]
pub mod data_type;
pub mod error;
pub mod column;

use std::collections::HashMap;
pub use data_type::*;
use odbc_api::buffers::TextRowSet;
use odbc_api::handles::StatementImpl;
use odbc_api::{Cursor, CursorImpl, ResultSetMetadata};
use std::str::FromStr;
use enumset::{EnumSet, EnumSetType};
use column::Table;

#[derive(Debug, Default)]
pub struct DmColumnDesc {
    table_id: Option<usize>,
    // inner: Vec<DmColumnInner>,
}

#[derive(Debug)]
pub struct DmColumnX {
    name: String,
    id: usize,
    colid: usize,
    r#type: usize,
    length: usize,
    scale: usize,
    nullable: bool,
    default_val: String,
    table_name: String,
}

pub trait DmAdapter {
    fn get_table_sql(table_name: &str) -> String;
    fn get_table_desc(self) -> anyhow::Result<Table>;
}

impl DmAdapter for CursorImpl<StatementImpl<'_>> {
    fn get_table_sql(table_name: &str) -> String {
        // Use sql: `SELECT A.*, B.NAME AS TABLE_NAME FROM SYSCOLUMNS AS a LEFT JOIN SYSOBJECTS AS B ON A.id = B.id WHERE B.name IN ("X")`;
        // The X is table name;
        format!(
            r#"SELECT A.*, B.NAME AS TABLE_NAME FROM SYSCOLUMNS AS a LEFT JOIN SYSOBJECTS AS B ON A.id = B.id WHERE B.name IN ({});"#,
            table_name
        )
    }

    fn get_table_desc(mut self) -> anyhow::Result<Table> {
        let headers = self.column_names()?.collect::<Result<Vec<String>, _>>()?;

        let mut buffers = TextRowSet::for_cursor(1024, &mut self, Some(4096))?;
        let mut row_set_cursor = self.bind_buffer(&mut buffers)?;
        let mut table_desc = DmColumnDesc::default();

        let mut data = vec![];
        while let Some(batch) = row_set_cursor.fetch()? {
            for row_index in 0..batch.num_rows() {
                let num_cols = batch.num_cols();
                let mut row_data: Vec<_> = (0..num_cols)
                    .map(|col_index| batch.at(col_index, row_index).unwrap_or(&[]))
                    .into_iter()
                    .map(String::from_utf8_lossy)
                    .map(|x|x.to_string())
                    .collect();
                data.push(row_data);
            }
        }
        let table = Table{ headers, data, };
        Ok(table)
    }
}
