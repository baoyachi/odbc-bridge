use crate::executor::Print;
use crate::extension::{Column, ColumnItem};
use crate::Convert;
use nu_table::{StyledString, Table, TableTheme, TextStyle};
use odbc_api::buffers::{AnyColumnView, BufferDescription, ColumnarAnyBuffer};
use odbc_api::{ColumnDescription, Connection, Cursor, ParameterCollectionRef, ResultSetMetadata};
use std::ops::IndexMut;

const BATCH_SIZE: usize = 5000;

#[derive(Debug, Default)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub data: Vec<Vec<ColumnItem>>,
}

impl Print for QueryResult {
    fn covert_table(&self) -> Table {
        let headers: Vec<StyledString> = self
            .columns
            .iter()
            .map(|x| StyledString::new(x.name.to_string(), TextStyle::default_header()))
            .collect();

        let rows = self
            .data
            .iter()
            .map(|x| {
                x.iter()
                    .map(|y| y.to_string())
                    .map(|y| StyledString::new(y, TextStyle::basic_left()))
                    .collect::<Vec<_>>()
            })
            .collect();
        Table::new(headers, rows, TableTheme::rounded())
    }
}

pub fn query_result<S: Into<String>>(
    conn: Connection,
    sql: S,
    max_batch_size: usize,
    params: impl ParameterCollectionRef,
) -> anyhow::Result<QueryResult> {
    let mut cursor = conn
        .execute(&sql.into(), params)?
        .ok_or_else(|| anyhow!("query error"))?;

    let mut query_result = QueryResult::default();
    for index in 0..cursor.num_result_cols()?.try_into()? {
        let mut column_description = ColumnDescription::default();
        cursor.describe_col(index + 1, &mut column_description)?;

        let column = Column::new(
            column_description.name_to_string()?,
            column_description.data_type,
            column_description.could_be_nullable(),
        );
        query_result.columns.push(column);
    }

    //
    let descs = query_result
        .columns
        .iter()
        .map(|c| <&Column as TryInto<BufferDescription>>::try_into(c).unwrap());

    let row_set_buffer = ColumnarAnyBuffer::from_description(max_batch_size, descs);

    let mut row_set_cursor = cursor.bind_buffer(row_set_buffer).unwrap();

    let mut total_row = vec![];
    while let Some(row_set) = row_set_cursor.fetch()? {
        for index in 0..query_result.columns.len() {
            let column_view: AnyColumnView = row_set.column(index);
            let column_types = column_view.convert();
            if index == 0 {
                for c in column_types.into_iter() {
                    total_row.push(vec![c]);
                }
            } else {
                for (col_index, c) in column_types.into_iter().enumerate() {
                    let row = total_row.index_mut(col_index);
                    row.push(c)
                }
            }
        }
    }
    query_result.data = total_row;
    Ok(query_result)
}
