use crate::executor::Print;
use bytes::Bytes;
use nu_table::{StyledString, Table, TableTheme, TextStyle};
use odbc_api::buffers::TextRowSet;
use odbc_api::{
    ColumnDescription, Connection, Cursor, DataType, ParameterCollectionRef, ResultSetMetadata,
};

#[derive(Debug, Default)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub data: Vec<Vec<Bytes>>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Column {
    fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }
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
                x.into_iter()
                    .map(|y| String::from_utf8_lossy(y.as_ref()).to_string())
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
    params: impl ParameterCollectionRef,
) -> anyhow::Result<QueryResult> {
    let mut query_result = QueryResult::default();
    let mut cursor = conn
        .execute(&sql.into(), params)?
        .ok_or_else(|| anyhow!("query error"))?;

    for col_index in 0..cursor.num_result_cols()? {
        let col_index_u16: u16 = (col_index + 1).try_into()?;
        let column_name = cursor.col_name(col_index_u16)?;
        let data_type = cursor.col_data_type(col_index_u16)?;

        let column = Column::new(column_name, data_type, false);
        query_result.columns.push(column);
    }

    const BATCH_SIZE: usize = 5000;
    let mut buffers = TextRowSet::for_cursor(BATCH_SIZE, &mut cursor, Some(4096))?;
    let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;
    while let Some(row_set) = row_set_cursor.fetch()? {
        for row_index in 0..row_set.num_rows() {
            let mut row_data = vec![];
            for col_index in 0..row_set.num_cols() {
                let msg_u8 = row_set.at(col_index, row_index).unwrap_or(&[]);
                let bytes = Bytes::copy_from_slice(msg_u8);
                row_data.push(bytes);
            }
            query_result.data.push(row_data);
        }
    }
    Ok(query_result)
}

pub fn query_result2<S: Into<String>>(
    conn: Connection,
    sql: S,
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

    Ok(query_result)
}
