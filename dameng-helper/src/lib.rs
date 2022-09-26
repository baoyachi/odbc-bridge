#[allow(non_camel_case_types)]
pub mod data_type;

use anyhow::anyhow;
use odbc_api::buffers::TextRowSet;
use odbc_api::{ColumnDescription, Cursor, CursorImpl, Error, ResultSetMetadata};
use odbc_api::handles::StatementImpl;
pub use data_type::*;

#[derive(Debug, Default)]
pub struct TableDesc {
    table_id: Option<usize>,
    inner: Vec<TableInner>,
}


#[derive(Debug, Default)]
pub struct TableInner {
    name: String,
    data_type: String,
}

impl TableInner {
    fn new(name: String, data_type: String) -> Self {
        Self { name, data_type }
    }
}


pub trait DmAdapter {
    fn get_table_sql(table_name: &str) -> String;
    fn get_table_desc(self) -> anyhow::Result<TableDesc>;
}

impl DmAdapter for CursorImpl<StatementImpl<'_>> {
    fn get_table_sql(table_name: &str) -> String {
        /// use sql: select a.name,a.type$ as data_type,a.id as table_id from SYSCOLUMNS as a left join SYSOBJECTS as b on a.id = b.id where b.name = '?'
        format!(
            r#"select a.name,a.type$ as data_type,a.id as table_id from SYSCOLUMNS as a left join SYSOBJECTS as b on a.id = b.id where b.name = '{}'"#,
            table_name)
    }

    fn get_table_desc(mut self) -> anyhow::Result<TableDesc> {
        let headers = self
            .column_names()?
            .collect::<Result<Vec<String>, _>>()?;

        assert_eq!(headers, vec!["name", "data_type", "table_id"]);

        let mut buffers = TextRowSet::for_cursor(1024, &mut self, Some(4096))?;
        let mut row_set_cursor = self.bind_buffer(&mut buffers)?;
        let mut table_desc = TableDesc::default();

        while let Some(batch) = row_set_cursor.fetch()? {
            for row_index in 0..batch.num_rows() {
                let num_cols = batch.num_cols();
                assert_eq!(num_cols, headers.len());

                let mut row_data: Vec<String> = (0..num_cols)
                    .map(|col_index| batch.at(col_index, row_index).unwrap_or(&[]))
                    .into_iter()
                    .map(|x| String::from_utf8_lossy(x).to_string())
                    .collect();
                table_desc.inner.push(TableInner::new(
                    row_data.remove(0),
                    row_data.remove(0),
                ));
                if table_desc.table_id.is_none() {
                    table_desc.table_id = Some(row_data.remove(0).parse::<usize>()?);
                }
            }
        }
        Ok(table_desc)
    }
}


