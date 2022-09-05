use crate::executor::Print;
use bytes::Bytes;
use nu_table::{StyledString, Table, TableTheme, TextStyle};
use odbc_api::buffers::{AnyColumnView, BufferDescription, BufferKind, ColumnarAnyBuffer, NullableSlice, TextRowSet};
use odbc_api::{ColumnDescription, Connection, Cursor, DataType, ParameterCollectionRef, ResultSetMetadata, RowSetBuffer};

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

impl TryFrom<&Column> for BufferDescription {
    type Error = String;

    fn try_from(c: &Column) -> Result<Self, Self::Error> {
        let description = BufferDescription {
            nullable: c.nullable,
            kind: BufferKind::from_data_type(c.data_type.clone()).ok_or_else(||
                format!("covert DataType:{:?} to BufferKind error", c.data_type)
            )?,
        };
        Ok(description)
    }
}

const BATCH_SIZE: usize = 5000;

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


#[derive(Debug, Default)]
pub struct QueryResult2<'a> {
    pub columns: Vec<Column>,
    pub data: Vec<Vec<AnyColumnView<'a>>>,
}


pub fn query_result2<S: Into<String>>(
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
    let descs = query_result.columns
        .iter()
        .map(|c| <&Column as TryInto<BufferDescription>>::try_into(c).unwrap());


    let row_set_buffer = ColumnarAnyBuffer::from_description(max_batch_size, descs);

    let mut row_set_cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    while let Some(row_set) = row_set_cursor.fetch()? {
        for row_index in 0..row_set.num_rows() {
            let mut column_views = vec![];
            for (index, column) in query_result.columns.iter().enumerate() {
                let column_view: AnyColumnView = row_set.column(index);
                column_views.push(column_view);
                println!("{}-{}", row_index, index);
            }
            // query_result.data.push(column_views);
        }
    }
    Ok(query_result)
}