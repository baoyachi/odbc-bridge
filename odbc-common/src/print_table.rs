use crate::error::OdbcStdResult;
use odbc_api::buffers::TextRowSet;
use odbc_api::Cursor;
use tabled::builder::Builder;
use tabled::{Style, Table};

pub trait Print: Sized {
    fn print_all_tables(self) -> OdbcStdResult<()> {
        let p = self.table_string()?;
        debug!("\n{}", p);
        Ok(())
    }

    fn header_data(self) -> OdbcStdResult<(Vec<String>, Vec<Vec<String>>)>;

    fn convert_table(self) -> OdbcStdResult<Table> {
        let (headers, records) = self.header_data()?;
        let mut builder = Builder::default();
        for record in records {
            builder.add_record(record);
        }
        builder.set_columns(headers);

        let mut table = builder.build();
        table.with(Style::modern());

        Ok(table)
    }

    fn table_string(self) -> OdbcStdResult<String> {
        let table = self.convert_table()?;
        let output = table.to_string();
        Ok(output)
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
