use std::error::Error;
use std::io::stdout;
use odbc_api::{Cursor, CursorImpl, ResultSetMetadata};
use odbc_api::buffers::TextRowSet;
use odbc_api::handles::StatementImpl;

const BATCH_SIZE: usize = 5000;

pub fn print_all_tables(mut cursor: CursorImpl<StatementImpl<'_>>) -> Result<(), Box<dyn Error>> {
    let out = stdout();
    let mut writer = csv::Writer::from_writer(out);
    // Write the column names to stdout
    let headline: Vec<String> = cursor.column_names()?.collect::<Result<_, _>>()?;
    writer.write_record(headline)?;

    // Use schema in cursor to initialize a text buffer large enough to hold the largest
    // possible strings for each column up to an upper limit of 4KiB.
    let mut buffers = TextRowSet::for_cursor(BATCH_SIZE, &mut cursor, Some(4096))?;
    // Bind the buffer to the cursor. It is now being filled with every call to fetch.
    let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;

    // Iterate over batches
    while let Some(batch) = row_set_cursor.fetch()? {
        // Within a batch, iterate over every row
        for row_index in 0..batch.num_rows() {
            // Within a row iterate over every column
            let record = (0..batch.num_cols()).map(|col_index| {
                batch
                    .at(col_index, row_index)
                    .unwrap_or(&[])
            });
            // Writes row as csv
            writer.write_record(record)?;
        }
    }
    Ok(())
}