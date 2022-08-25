use nu_protocol::Config;
use nu_table::{Alignments, StyledString, Table, TableTheme, TextStyle};
use odbc_api::buffers::TextRowSet;
use odbc_api::Cursor;
use std::collections::HashMap;
use std::error::Error;

const BATCH_SIZE: usize = 5000;

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
pub fn print_all_tables(cursor: impl Cursor) -> Result<(), Box<dyn Error>> {
    let table = convert_table(cursor)?;
    let string = convert_table_string(table).ok_or("convert table to string error")?;
    debug!("\n{}", string);
    Ok(())
}

pub fn convert_table(mut cursor: impl Cursor) -> Result<Table, Box<dyn Error>> {
    let headers: Vec<StyledString> = cursor
        .column_names()?
        .collect::<Result<Vec<String>, _>>()?
        .into_iter()
        .map(|x| StyledString::new(x, TextStyle::default_header()))
        .collect();

    // Use schema in cursor to initialize a text buffer large enough to hold the largest
    // possible strings for each column up to an upper limit of 4KiB.
    let mut buffers = TextRowSet::for_cursor(BATCH_SIZE, &mut cursor, Some(4096))?;
    // Bind the buffer to the cursor. It is now being filled with every call to fetch.
    let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;
    let mut rows = vec![];
    // Iterate over batches
    while let Some(batch) = row_set_cursor.fetch()? {
        // Within a batch, iterate over every row
        for row_index in 0..batch.num_rows() {
            // Within a row iterate over every column
            let row_data = (0..batch.num_cols())
                .map(|col_index| batch.at(col_index, row_index).unwrap_or(&[]))
                .into_iter()
                .map(|x| String::from_utf8_lossy(x).to_string())
                .map(|x| StyledString::new(x, TextStyle::basic_left()))
                .collect();
            rows.push(row_data);
        }
    }
    let table = Table::new(headers, rows, TableTheme::rounded());

    Ok(table)
}

pub fn convert_table_string(table: Table) -> Option<String> {
    let cfg = Config::default();
    let styles = HashMap::default();
    let alignments = Alignments::default();

    table.draw_table(&cfg, &styles, alignments, usize::MAX)
}
