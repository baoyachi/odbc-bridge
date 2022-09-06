use std::char::decode_utf16;
use crate::executor::Print;
use bytes::Bytes;
use nu_table::{StyledString, Table, TableTheme, TextStyle};
use odbc_api::buffers::{AnyColumnView, BufferDescription, BufferKind, ColumnarAnyBuffer, NullableSlice, TextRowSet};
use odbc_api::{Bit, ColumnDescription, Connection, Cursor, DataType, ParameterCollectionRef, ResultSetMetadata, RowSetBuffer};
use odbc_api::sys::*;

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
pub struct QueryResult2 {
    pub columns: Vec<Column>,
    pub data: Vec<Vec<ColumnType>>,
}

impl Print for QueryResult2 {
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
                    .map(|y| y.to_string())
                    .map(|y| StyledString::new(y, TextStyle::basic_left()))
                    .collect::<Vec<_>>()
            })
            .collect();
        Table::new(headers, rows, TableTheme::rounded())
    }
}


pub fn query_result2<S: Into<String>>(
    conn: Connection,
    sql: S,
    max_batch_size: usize,
    params: impl ParameterCollectionRef,
) -> anyhow::Result<QueryResult2> {
    let mut cursor = conn
        .execute(&sql.into(), params)?
        .ok_or_else(|| anyhow!("query error"))?;

    let mut query_result = QueryResult2::default();
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
        let total_row: Vec<Vec<ColumnType>> = vec![];
        // let single_row: Vec<ColumnType=vec![];
        for (index, column) in query_result.columns.iter().enumerate() {
            let column_view: AnyColumnView = row_set.column(index);
            let column_type = column_view.convert_struct_type();
            println!("println:len:{},{}-{:?}", column_type.len(), index, column_type);
            query_result.data.push(column_type);
        }
    }
    Ok(query_result)
}


#[derive(Debug)]
pub enum ColumnType {
    Text(Option<String>),
    WText(Option<String>),
    Binary(Option<Vec<u8>>),
    Date(Option<Date>),
    Time(Option<Time>),
    Timestamp(Option<Timestamp>),
    F64(Option<f64>),
    F32(Option<f32>),
    I8(Option<i8>),
    I16(Option<i16>),
    I32(Option<i32>),
    I64(Option<i64>),
    U8(Option<u8>),
    Bit(Option<bool>),

}


pub trait ConvertType {
    fn convert_struct_type(self) -> Vec<ColumnType>;
}

impl ConvertType for AnyColumnView<'_> {
    fn convert_struct_type(self) -> Vec<ColumnType> {
        match self {
            AnyColumnView::Text(view) => {
                let mut buffer = Vec::with_capacity(view.len());
                for v in view.iter() {
                    if let Some(x) = v {
                        let cow = String::from_utf8_lossy(x);
                        buffer.push(ColumnType::Text(Some(cow.to_string())));
                    } else {
                        buffer.push(ColumnType::Text(None))
                    }
                }
                return buffer;
            }
            AnyColumnView::WText(view) => {
                let mut buffer = Vec::with_capacity(view.len());
                for value in view.iter() {
                    if let Some(utf16) = value {
                        let mut buf_utf8 = String::new();
                        for c in decode_utf16(utf16.as_slice().iter().cloned()) {
                            buf_utf8.push(c.unwrap());
                        }
                        buffer.push(ColumnType::WText(Some(buf_utf8)));
                    } else {
                        buffer.push(ColumnType::WText(None))
                    }
                }
                return buffer;
            }
            AnyColumnView::Binary(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    if let Some(bytes) = value {
                        buffer.push(ColumnType::Binary(Some(bytes.to_vec())))
                    } else {
                        buffer.push(ColumnType::Binary(None))
                    }
                }
                return buffer;
            }
            AnyColumnView::Date(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnType::Date(Some(value.clone())))
                }
                return buffer;
            }
            AnyColumnView::Timestamp(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnType::Timestamp(Some(value.clone())))
                }
                return buffer;
            }
            AnyColumnView::Time(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnType::Time(Some(value.clone())))
                }
                return buffer;
            }
            AnyColumnView::I32(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnType::I32(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::Bit(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnType::Bit(Some(value.as_bool())))
                }
                return buffer;
            }

            AnyColumnView::F64(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnType::F64(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::F32(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnType::F32(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::I8(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnType::I8(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::I16(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnType::I16(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::I64(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnType::I64(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::U8(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnType::U8(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::NullableDate(_) => { warn!("lost NullableDate type"); }
            AnyColumnView::NullableTime(_) => { warn!("lost NullableTime type"); }
            AnyColumnView::NullableTimestamp(_) => { warn!("lost NullableTimestamp type"); }
            AnyColumnView::NullableF64(_) => { warn!("lost NullableF64 type"); }
            AnyColumnView::NullableF32(_) => { warn!("lost NullableF32 type"); }
            AnyColumnView::NullableI8(_) => { warn!("lost NullableI8 type"); }
            AnyColumnView::NullableI16(_) => { warn!("lost NullableI16 type"); }
            AnyColumnView::NullableI32(_) => { warn!("lost NullableI32 type"); }
            AnyColumnView::NullableI64(_) => { warn!("lost NullableI64 type"); }
            AnyColumnView::NullableU8(_) => { warn!("lost NullableU8 type"); }
            AnyColumnView::NullableBit(_) => { warn!("lost NullableBit type"); }
        };
        vec![ColumnType::Bit(None)]
    }
}

impl ToString for ColumnType {
    fn to_string(&self) -> String {
        format!("{:?}",self)
    }
}