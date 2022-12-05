use nu_protocol::Config;
use nu_table::Table;
use nu_table::{Alignments, StyledString, TableTheme, TextStyle};
use odbc_api::buffers::TextRowSet;
use odbc_api::Cursor;
use std::collections::HashMap;

use crate::error::{OdbcStdError, OdbcStdResult};

pub trait Print: Sized {
    fn print_all_tables(self) -> OdbcStdResult<()> {
        let p = self.table_string()?;
        debug!("\n{}", p);
        Ok(())
    }

    fn header_data(self) -> OdbcStdResult<(Vec<String>, Vec<Vec<String>>)>;

    fn convert_table(self) -> OdbcStdResult<Table> {
        let (headers, data) = self.header_data()?;
        let headers: Vec<StyledString> = headers
            .into_iter()
            .map(|x| StyledString::new(x, TextStyle::default_header()))
            .collect();

        let rows = data
            .iter()
            .map(|x| {
                x.iter()
                    .map(|x| StyledString::new(x.to_string(), TextStyle::basic_left()))
                    .collect()
            })
            .collect::<Vec<Vec<StyledString>>>();

        Ok(Table::new(headers, rows, TableTheme::rounded()))
    }

    fn table_string(self) -> OdbcStdResult<String> {
        let table = self.convert_table()?;
        let cfg = Config::default();
        let styles = HashMap::default();
        let alignments = Alignments::default();

        let p = table
            .draw_table(&cfg, &styles, alignments, usize::MAX)
            .ok_or_else(|| {
                OdbcStdError::TypeConversionError("convert table to string error".to_string())
            })?;
        Ok(p)
    }
}

const BATCH_SIZE: usize = 128;

/// Print Cursor output to table.E.g:
/// ```bash
/// > run you code...
/// ╭────┬────────────┬────────────────────────────┬────────────────────────────╮
/// │ id │   name     │         created_at         │         updated_at         │
/// ├────┼────────────┼────────────────────────────┼────────────────────────────┤
/// │ 1  │   hallo    │ 2022-08-24 15:50:36.000000 │ 2022-08-24 15:50:36.000000 │
/// ╰────┴────────────┴────────────────────────────┴────────────────────────────╯
// ```
///
impl<T> Print for T
where
    T: Cursor,
{
    fn header_data(mut self) -> OdbcStdResult<(Vec<String>, Vec<Vec<String>>)> {
        let headers: Vec<String> = self.column_names()?.collect::<Result<Vec<String>, _>>()?;

        // Use schema in cursor to initialize a text buffer large enough to hold the largest
        // possible strings for each column up to an upper limit of 4KiB.
        let mut buffers = TextRowSet::for_cursor(BATCH_SIZE, &mut self, Some(4096))?;
        // Bind the buffer to the cursor. It is now being filled with every call to fetch.
        let mut row_set_cursor = self.bind_buffer(&mut buffers)?;
        let mut data = vec![];
        // Iterate over batches
        while let Some(batch) = row_set_cursor.fetch()? {
            // Within a batch, iterate over every row
            for row_index in 0..batch.num_rows() {
                // Within a row iterate over every column
                let row_data = (0..batch.num_cols())
                    .map(|col_index| batch.at(col_index, row_index).unwrap_or(&[]))
                    .into_iter()
                    .map(|x| String::from_utf8_lossy(x).to_string())
                    .collect();
                data.push(row_data);
            }
        }
        Ok((headers, data))
    }
}
