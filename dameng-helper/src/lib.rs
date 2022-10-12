#![deny(missing_debug_implementations)]

#[allow(non_camel_case_types)]
pub mod data_type;
pub mod error;
pub mod table;

pub use data_type::*;
use odbc_api::buffers::TextRowSet;
use odbc_api::handles::StatementImpl;
use odbc_api::{Cursor, CursorImpl, ResultSetMetadata};

pub trait DmAdapter {
    fn get_table_sql(table_name: Vec<String>) -> String;
    fn get_table_desc(self) -> anyhow::Result<(Vec<String>, Vec<Vec<String>>)>;
}

impl DmAdapter for CursorImpl<StatementImpl<'_>> {
    fn get_table_sql(table_name: Vec<String>) -> String {
        // Use sql: `SELECT A.*, B.NAME AS TABLE_NAME FROM SYSCOLUMNS AS a LEFT JOIN SYSOBJECTS AS B ON A.id = B.id WHERE B.name IN ("X")`;
        // The X is table name;
        let table_name = table_name
            .iter()
            .map(|x| format!("'{}'", x))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            r#"SELECT A.NAME, A.ID, A.COLID, A.TYPE$, A.LENGTH$, A.SCALE, A.NULLABLE$, A.DEFVAL, B.NAME AS TABLE_NAME, B.CRTDATE FROM SYSCOLUMNS AS a LEFT JOIN SYSOBJECTS AS B ON A.id = B.id WHERE B.name IN ({});"#,
            table_name
        )
    }

    fn get_table_desc(mut self) -> anyhow::Result<(Vec<String>, Vec<Vec<String>>)> {
        let headers = self.column_names()?.collect::<Result<Vec<_>, _>>()?;

        let mut buffers = TextRowSet::for_cursor(1024, &mut self, Some(4096))?;
        let mut row_set_cursor = self.bind_buffer(&mut buffers)?;

        let mut data = vec![];
        while let Some(batch) = row_set_cursor.fetch()? {
            for row_index in 0..batch.num_rows() {
                let num_cols = batch.num_cols();
                let row_data: Vec<_> = (0..num_cols)
                    .map(|col_index| batch.at(col_index, row_index).unwrap_or(&[]))
                    .into_iter()
                    .map(String::from_utf8_lossy)
                    .map(|x| x.to_string())
                    .collect();
                data.push(row_data);
            }
        }
        Ok((headers, data))
    }
}
