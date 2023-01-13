use crate::executor::query::OdbcRow;
use crate::{Convert, TryConvert};
use bytes::BytesMut;
use odbc_common::error::OdbcStdResult;
use odbc_common::odbc_api::buffers::Indicator;
use odbc_common::odbc_api::handles::{ParameterDescription, Statement, StatementRef};
use odbc_common::odbc_api::parameter::{VarBinaryBox, VarCharBox, VarCharSliceMut};
use odbc_common::odbc_api::{
    buffers::{AnySlice, BufferDesc},
    sys::{Date, Time, Timestamp, NULL_DATA},
    DataType,
};
use odbc_common::odbc_api::{Bit, ColumnDescription, CursorRow, Nullability, Nullable};
use std::cmp::max;

#[derive(Debug, Clone)]
pub struct OdbcColumnDesc {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct OdbcParamDesc {
    pub data_type: DataType,
    pub nullable: bool,
}

impl OdbcColumnDesc {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }
}

impl TryFrom<ParameterDescription> for OdbcParamDesc {
    type Error = odbc_common::error::OdbcStdError;

    fn try_from(description: ParameterDescription) -> Result<Self, Self::Error> {
        let nullable = match description.nullable {
            Nullability::Nullable | Nullability::Unknown => true,
            Nullability::NoNulls => false,
        };

        Ok(OdbcParamDesc {
            data_type: description.data_type,
            nullable,
        })
    }
}

impl TryFrom<ColumnDescription> for OdbcColumnDesc {
    type Error = odbc_common::error::OdbcStdError;

    fn try_from(column_description: ColumnDescription) -> Result<Self, Self::Error> {
        Ok(OdbcColumnDesc::new(
            column_description.name_to_string()?,
            column_description.data_type,
            column_description.could_be_nullable(),
        ))
    }
}

impl TryConvert<BufferDesc> for &OdbcColumnDesc {
    type Error = String;

    fn try_convert(self) -> Result<BufferDesc, Self::Error> {
        let desc = BufferDesc::from_data_type(self.data_type, self.nullable)
            .ok_or_else(|| format!("covert DataType:{:?} to BufferDesc error", self.data_type))?;
        Ok(desc)
    }
}

#[allow(missing_debug_implementations)]
pub struct OdbcColsBuf {
    pub columns: Vec<(u16, OdbcDataBuf)>,
}

impl OdbcColsBuf {
    pub fn is_long_data<'a>(
        mut col_desc: impl Iterator<Item = &'a OdbcColumnDesc>,
        max_str_len: usize,
        max_binary_len: usize,
    ) -> bool {
        col_desc.any(|i| match i.data_type {
            odbc_common::odbc_api::DataType::Char { length }
            | odbc_common::odbc_api::DataType::WChar { length }
            | odbc_common::odbc_api::DataType::Varchar { length }
            | odbc_common::odbc_api::DataType::WVarchar { length }
            | odbc_common::odbc_api::DataType::LongVarchar { length } => length > max_str_len,
            odbc_common::odbc_api::DataType::LongVarbinary { length }
            | odbc_common::odbc_api::DataType::Varbinary { length }
            | odbc_common::odbc_api::DataType::Binary { length } => length > max_binary_len,
            _ => false,
        })
    }

    pub fn from_descs(
        descs: impl Iterator<Item = BufferDesc>,
        max_str_len: usize,
        max_binary_len: usize,
    ) -> Self {
        let mut columns = Vec::new();
        let mut col_num = 0;
        for desc in descs {
            let buf = OdbcDataBuf::from_desc(desc, max_str_len, max_binary_len);
            col_num += 1;
            columns.push((col_num, buf));
        }
        Self { columns }
    }

    pub fn bind_col(&mut self, cursor: &mut StatementRef<'_>) -> OdbcStdResult<()> {
        for (col_num, col_buf) in &mut self.columns {
            unsafe {
                col_buf.bind_col(*col_num, cursor)?;
            }
        }
        Ok(())
    }

    pub fn get_row(&mut self, cursor_row: &mut CursorRow) -> OdbcStdResult<OdbcRow> {
        let mut columns = Vec::with_capacity(self.columns.len());

        for (col_num, col_buf) in &mut self.columns {
            columns.push(col_buf.get_data(*col_num, cursor_row)?)
        }

        Ok(columns)
    }
}

#[allow(missing_debug_implementations)]
pub enum OdbcDataBuf {
    BigText(Vec<u8>),
    BigWText(Vec<u8>),
    BigBinary(Vec<u8>),
    Text(VarCharBox),
    WText(VarCharBox),
    Binary(VarBinaryBox),
    Date(Nullable<Date>),
    Time(Nullable<Time>),
    Timestamp(Nullable<Timestamp>),
    F64(Nullable<f64>),
    F32(Nullable<f32>),
    I8(Nullable<i8>),
    I16(Nullable<i16>),
    I32(Nullable<i32>),
    I64(Nullable<i64>),
    U8(Nullable<u8>),
    Bit(Nullable<Bit>),
}

impl OdbcDataBuf {
    pub fn from_desc(desc: BufferDesc, max_str_len: usize, max_binary_len: usize) -> Self {
        const UTF8_MAX_BYTE: usize = 4;
        match desc {
            BufferDesc::Binary { length } => {
                if length <= max_binary_len {
                    OdbcDataBuf::Binary(VarBinaryBox::from_vec(vec![0u8; length]))
                } else {
                    OdbcDataBuf::BigBinary(vec![0u8; max(max_binary_len, 1)])
                }
            }
            BufferDesc::Text {
                max_str_len: length,
            } => {
                if length <= max_str_len {
                    OdbcDataBuf::Text(VarCharBox::from_vec(vec![0u8; length + 1]))
                } else {
                    OdbcDataBuf::BigText(vec![0u8; max(max_str_len, UTF8_MAX_BYTE) + 1])
                }
            }
            BufferDesc::WText {
                max_str_len: length,
            } => {
                if length <= max_str_len {
                    OdbcDataBuf::WText(VarCharBox::from_vec(vec![0u8; length + 1]))
                } else {
                    OdbcDataBuf::BigWText(vec![0u8; max(max_str_len, UTF8_MAX_BYTE) + 1])
                }
            }
            BufferDesc::F64 { .. } => OdbcDataBuf::F64(Nullable::<f64>::null()),
            BufferDesc::F32 { .. } => OdbcDataBuf::F32(Nullable::<f32>::null()),
            BufferDesc::Date { .. } => OdbcDataBuf::Date(Nullable::<Date>::null()),
            BufferDesc::Time { .. } => OdbcDataBuf::Time(Nullable::<Time>::null()),
            BufferDesc::Timestamp { .. } => OdbcDataBuf::Timestamp(Nullable::<Timestamp>::null()),
            BufferDesc::I8 { .. } => OdbcDataBuf::I8(Nullable::<i8>::null()),
            BufferDesc::I16 { .. } => OdbcDataBuf::I16(Nullable::<i16>::null()),
            BufferDesc::I32 { .. } => OdbcDataBuf::I32(Nullable::<i32>::null()),
            BufferDesc::I64 { .. } => OdbcDataBuf::I64(Nullable::<i64>::null()),
            BufferDesc::U8 { .. } => OdbcDataBuf::U8(Nullable::<u8>::null()),
            BufferDesc::Bit { .. } => OdbcDataBuf::Bit(Nullable::<Bit>::null()),
        }
    }

    /// # Safety
    ///
    /// It is the callers responsibility to make sure the bound columns live until they are no
    /// longer bound.
    pub unsafe fn bind_col(
        &mut self,
        column_number: u16,
        cursor: &mut StatementRef<'_>,
    ) -> OdbcStdResult<()> {
        match self {
            OdbcDataBuf::Date(buf) => cursor.bind_col(column_number, buf).into_result(cursor)?,
            OdbcDataBuf::Time(buf) => cursor.bind_col(column_number, buf).into_result(cursor)?,
            OdbcDataBuf::Timestamp(buf) => {
                cursor.bind_col(column_number, buf).into_result(cursor)?
            }
            OdbcDataBuf::F64(buf) => cursor.bind_col(column_number, buf).into_result(cursor)?,
            OdbcDataBuf::F32(buf) => cursor.bind_col(column_number, buf).into_result(cursor)?,
            OdbcDataBuf::I8(buf) => cursor.bind_col(column_number, buf).into_result(cursor)?,
            OdbcDataBuf::I16(buf) => cursor.bind_col(column_number, buf).into_result(cursor)?,
            OdbcDataBuf::I32(buf) => cursor.bind_col(column_number, buf).into_result(cursor)?,
            OdbcDataBuf::I64(buf) => cursor.bind_col(column_number, buf).into_result(cursor)?,
            OdbcDataBuf::U8(buf) => cursor.bind_col(column_number, buf).into_result(cursor)?,
            OdbcDataBuf::Bit(buf) => cursor.bind_col(column_number, buf).into_result(cursor)?,

            OdbcDataBuf::Text(buf) => cursor.bind_col(column_number, buf).into_result(cursor)?,
            OdbcDataBuf::WText(buf) => cursor.bind_col(column_number, buf).into_result(cursor)?,
            OdbcDataBuf::Binary(buf) => cursor.bind_col(column_number, buf).into_result(cursor)?,

            OdbcDataBuf::BigText(_) | OdbcDataBuf::BigWText(_) | OdbcDataBuf::BigBinary(_) => {}
        };
        Ok(())
    }

    pub fn get_data(
        &mut self,
        column_number: u16,
        cursor_row: &mut CursorRow,
    ) -> OdbcStdResult<OdbcColumnItem> {
        let result = match self {
            OdbcDataBuf::Date(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::Date,
                value: buf.as_opt().map(|value| {
                    bytes::BytesMut::from(value.try_convert().unwrap().to_string().as_bytes())
                }),
            },
            OdbcDataBuf::Time(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::Time,
                value: buf.as_opt().map(|value| {
                    bytes::BytesMut::from(value.try_convert().unwrap().to_string().as_bytes())
                }),
            },
            OdbcDataBuf::Timestamp(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::Timestamp,
                value: buf.as_opt().map(|value| {
                    bytes::BytesMut::from(
                        <Timestamp as TryConvert<time::PrimitiveDateTime>>::try_convert(*value)
                            .unwrap()
                            .to_string()
                            .as_bytes(),
                    )
                }),
            },
            OdbcDataBuf::F64(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::F64,
                value: buf
                    .as_opt()
                    .map(|value| bytes::BytesMut::from(value.to_string().as_bytes())),
            },
            OdbcDataBuf::F32(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::F32,
                value: buf
                    .as_opt()
                    .map(|value| bytes::BytesMut::from(value.to_string().as_bytes())),
            },
            OdbcDataBuf::I8(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::I8,
                value: buf
                    .as_opt()
                    .map(|value| bytes::BytesMut::from(value.to_string().as_bytes())),
            },
            OdbcDataBuf::I16(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::I16,
                value: buf
                    .as_opt()
                    .map(|value| bytes::BytesMut::from(value.to_string().as_bytes())),
            },
            OdbcDataBuf::I32(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::I32,
                value: buf
                    .as_opt()
                    .map(|value| bytes::BytesMut::from(value.to_string().as_bytes())),
            },
            OdbcDataBuf::I64(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::I64,
                value: buf
                    .as_opt()
                    .map(|value| bytes::BytesMut::from(value.to_string().as_bytes())),
            },
            OdbcDataBuf::U8(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::U8,
                value: buf
                    .as_opt()
                    .map(|value| BytesMut::from(vec![*value].as_slice())),
            },
            OdbcDataBuf::Bit(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::Bit,
                value: buf
                    .as_opt()
                    .map(|value| BytesMut::from(value.as_bool().to_string().as_bytes())),
            },
            OdbcDataBuf::Text(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::Text,
                value: buf.as_bytes().map(BytesMut::from),
            },
            OdbcDataBuf::WText(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::WText,
                value: buf.as_bytes().map(BytesMut::from),
            },
            OdbcDataBuf::Binary(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::Binary,
                value: buf.as_bytes().map(BytesMut::from),
            },
            OdbcDataBuf::BigText(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::Text,
                value: Self::get_dameng_long_text(column_number, buf, cursor_row)?,
            },
            OdbcDataBuf::BigWText(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::WText,
                value: Self::get_dameng_long_text(column_number, buf, cursor_row)?,
            },
            OdbcDataBuf::BigBinary(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::Binary,
                value: Self::get_long_binary(column_number, buf, cursor_row)?,
            },
        };
        Ok(result)
    }

    pub fn get_dameng_long_text(
        column_number: u16,
        buf: &mut Vec<u8>,
        cursor_row: &mut CursorRow,
    ) -> OdbcStdResult<Option<BytesMut>> {
        const INVALID_UTF8_BYTE: u8 = 0xFF;
        const NULL_BYTE: u8 = 0x00;
        const MAX_INVALID_UTF8_BYTE_LENGTH: usize = 3;

        let reset_buf = |buffer: &mut [u8]| {
            // buf_length 比 data_length 大1才不会截断.
            // 达梦截断是按照字符而不是字节去截断的, 一个utf8最多占4个字节, 所以可能buf里的倒数第234字节, 总共三个字节没有被使用到(倒数第一个字节总是0).
            // 3+1=4(3个可能未使用的utf8字节和1个截断字节), 这4个字节都可能被驱动设为截断字符, 将其赋值为任意非截断字符.
            if let Some(end) = buffer.rchunks_mut(MAX_INVALID_UTF8_BYTE_LENGTH + 1).next() {
                for i in end {
                    // 可以是任意非0(非NULL_BYTE)字符
                    *i = INVALID_UTF8_BYTE;
                }
            }
        };

        let trim_null_or_invalid_utf8 = |data: &[u8]| -> usize {
            let mut trim_len = 0;
            if let Some(end) = data.rchunks(MAX_INVALID_UTF8_BYTE_LENGTH + 1).next() {
                for item in end.iter().rev() {
                    if *item == NULL_BYTE {
                        trim_len += 1;
                        break;
                    } else {
                        trim_len += 1;
                    }
                }
            }
            trim_len
        };

        let mut result = Option::<bytes::BytesMut>::None;
        let mut set_result = |v: &[u8]| match result {
            Some(ref mut actual) => {
                actual.extend_from_slice(v);
            }
            None => {
                result = Some(bytes::BytesMut::from(v));
            }
        };

        let resize_buf = |v: &mut Vec<u8>, new_len: usize| {
            let len = v.len();

            if new_len > len {
                v.reserve_exact(new_len - len);
                // # Safety
                // - `new_len` equal to [`capacity()`].
                unsafe {
                    v.set_len(v.capacity());
                }
            }
        };

        resize_buf(buf, buf.capacity());

        // We repeatedly fetch data and add it to the buffer. The buffer length is therefore the
        // accumulated value size. This variable keeps track of the number of bytes we added with
        // the next call to get_data.
        let mut fetch_size = buf.len();
        reset_buf(buf);
        let mut target = VarCharSliceMut::from_buffer(buf.as_mut_slice(), Indicator::Null);
        // Fetch binary data into buffer.
        cursor_row.get_data(column_number, &mut target)?;

        loop {
            match target.indicator() {
                // Value is `NULL`. We are done here.
                Indicator::Null => {
                    break;
                }
                // We do not know how large the value is. Let's fetch the data with repeated calls
                // to get_data.
                Indicator::NoTotal => {
                    let trim_len = trim_null_or_invalid_utf8(buf);
                    set_result(&buf[..fetch_size - trim_len]);

                    // Use an exponential strategy for increasing buffer size.
                    resize_buf(buf, buf.len() * 2);

                    fetch_size = buf.len();
                    reset_buf(buf);
                    target = VarCharSliceMut::from_buffer(buf, Indicator::Null);
                    cursor_row.get_data(column_number, &mut target)?;
                }
                // We did get the complete value, including the terminating zero. Let's resize the
                // buffer to match the retrieved value exactly (excluding terminating zero).
                Indicator::Length(len) if len < fetch_size => {
                    // Since the indicator refers to value length without terminating zero, this
                    // also implicitly drops the terminating zero at the end of the buffer.
                    let shrink_by = fetch_size - len;
                    set_result(&buf[..buf.len() - shrink_by]);
                    break;
                }
                // We did not get all of the value in one go, but the data source has been friendly
                // enough to tell us how much is missing.
                Indicator::Length(len) => {
                    let trim_len = trim_null_or_invalid_utf8(buf);
                    set_result(&buf[..fetch_size - trim_len]);

                    resize_buf(buf, len + 1);

                    fetch_size = buf.len();
                    reset_buf(buf);
                    target = VarCharSliceMut::from_buffer(buf, Indicator::Null);
                    cursor_row.get_data(column_number, &mut target)?;
                }
            }
        }
        Ok(result)
    }

    pub fn get_long_text(
        column_number: u16,
        buf: &mut Vec<u8>,
        cursor_row: &mut CursorRow,
    ) -> OdbcStdResult<Option<BytesMut>> {
        let is_null = cursor_row.get_text(column_number, buf)?;
        Ok(is_null.then(|| bytes::BytesMut::from(buf.as_slice())))
    }

    pub fn get_long_binary(
        column_number: u16,
        buf: &mut Vec<u8>,
        cursor_row: &mut CursorRow,
    ) -> OdbcStdResult<Option<BytesMut>> {
        let is_null = cursor_row.get_binary(column_number, buf)?;
        Ok(is_null.then(|| bytes::BytesMut::from(buf.as_slice())))
    }
}

#[derive(Debug)]
pub struct OdbcColumnItem {
    pub odbc_type: OdbcColumnType,
    pub value: Option<BytesMut>,
}

#[derive(Debug)]
pub enum OdbcColumnType {
    Text,
    WText,
    Binary,
    Date,
    Time,
    Timestamp,
    F64,
    F32,
    I8,
    I16,
    I32,
    I64,
    U8,
    Bit,
}

impl ToString for OdbcColumnItem {
    fn to_string(&self) -> String {
        format!("{self:?}")
    }
}

impl Convert<Vec<OdbcColumnItem>> for AnySlice<'_> {
    fn convert(self) -> Vec<OdbcColumnItem> {
        match self {
            AnySlice::Text(view) => {
                let mut buffer = Vec::with_capacity(view.len());
                for v in view.iter() {
                    if let Some(x) = v {
                        buffer.push(OdbcColumnItem {
                            odbc_type: OdbcColumnType::Text,
                            value: Some(BytesMut::from(x)),
                        });
                    } else {
                        buffer.push(OdbcColumnItem {
                            odbc_type: OdbcColumnType::Text,
                            value: None,
                        })
                    }
                }
                buffer
            }
            AnySlice::WText(view) => {
                let mut buffer = Vec::with_capacity(view.len());
                for v in view.iter() {
                    if let Some(x) = v {
                        buffer.push(OdbcColumnItem {
                            odbc_type: OdbcColumnType::WText,
                            value: Some(BytesMut::from(x.to_string().unwrap().as_bytes())),
                        });
                    } else {
                        buffer.push(OdbcColumnItem {
                            odbc_type: OdbcColumnType::WText,
                            value: None,
                        })
                    }
                }
                buffer
            }
            AnySlice::Binary(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    if let Some(bytes) = value {
                        buffer.push(OdbcColumnItem {
                            odbc_type: OdbcColumnType::Binary,
                            value: Some(BytesMut::from(bytes)),
                        })
                    } else {
                        buffer.push(OdbcColumnItem {
                            odbc_type: OdbcColumnType::Binary,
                            value: None,
                        })
                    }
                }
                buffer
            }
            AnySlice::Date(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    let val = value.try_convert().unwrap();
                    buffer.push(OdbcColumnItem {
                        odbc_type: OdbcColumnType::Date,
                        value: Some(BytesMut::from(val.to_string().as_bytes())),
                    })
                }
                buffer
            }
            AnySlice::Timestamp(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    let val: time::PrimitiveDateTime = value.try_convert().unwrap();
                    buffer.push(OdbcColumnItem {
                        odbc_type: OdbcColumnType::Timestamp,
                        value: Some(BytesMut::from(val.to_string().as_bytes())),
                    })
                }
                buffer
            }
            AnySlice::Time(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    let val = value.try_convert().unwrap();
                    buffer.push(OdbcColumnItem {
                        odbc_type: OdbcColumnType::Time,
                        value: Some(BytesMut::from(val.to_string().as_bytes())),
                    })
                }
                buffer
            }
            AnySlice::I32(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem {
                        odbc_type: OdbcColumnType::I32,
                        value: Some(BytesMut::from(value.to_string().as_bytes())),
                    })
                }
                buffer
            }
            AnySlice::Bit(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem {
                        odbc_type: OdbcColumnType::Bit,
                        value: Some(BytesMut::from(value.as_bool().to_string().as_bytes())),
                    })
                }
                buffer
            }

            AnySlice::F64(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem {
                        odbc_type: OdbcColumnType::F64,
                        value: Some(BytesMut::from(value.to_string().as_bytes())),
                    })
                }
                buffer
            }
            AnySlice::F32(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem {
                        odbc_type: OdbcColumnType::F32,
                        value: Some(BytesMut::from(value.to_string().as_bytes())),
                    })
                }
                buffer
            }
            AnySlice::I8(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem {
                        odbc_type: OdbcColumnType::I8,
                        value: Some(BytesMut::from(value.to_string().as_bytes())),
                    })
                }
                buffer
            }
            AnySlice::I16(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem {
                        odbc_type: OdbcColumnType::I16,
                        value: Some(BytesMut::from(value.to_string().as_bytes())),
                    })
                }
                buffer
            }
            AnySlice::I64(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem {
                        odbc_type: OdbcColumnType::I64,
                        value: Some(BytesMut::from(value.to_string().as_bytes())),
                    })
                }
                buffer
            }
            AnySlice::U8(view) => {
                let mut buffer = vec![];

                for value in view.iter() {
                    buffer.push(OdbcColumnItem {
                        odbc_type: OdbcColumnType::U8,
                        value: Some(BytesMut::from(vec![*value].as_slice())),
                    })
                }
                buffer
            }
            AnySlice::NullableDate(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            let val = value.try_convert().unwrap();
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::Date,
                                value: Some(BytesMut::from(val.to_string().as_bytes())),
                            }
                        } else {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::Date,
                                value: None,
                            }
                        }
                    })
                    .collect()
            }
            AnySlice::NullableTime(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            let val = value.try_convert().unwrap();
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::Time,
                                value: Some(BytesMut::from(val.to_string().as_bytes())),
                            }
                        } else {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::Time,
                                value: None,
                            }
                        }
                    })
                    .collect()
            }
            AnySlice::NullableTimestamp(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            let val: time::PrimitiveDateTime = value.try_convert().unwrap();
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::Timestamp,
                                value: Some(BytesMut::from(val.to_string().as_bytes())),
                            }
                        } else {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::Timestamp,
                                value: None,
                            }
                        }
                    })
                    .collect()
            }
            AnySlice::NullableF64(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::F64,
                                value: Some(BytesMut::from(value.to_string().as_bytes())),
                            }
                        } else {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::F64,
                                value: None,
                            }
                        }
                    })
                    .collect()
            }
            AnySlice::NullableF32(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::F32,
                                value: Some(BytesMut::from(value.to_string().as_bytes())),
                            }
                        } else {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::F32,
                                value: None,
                            }
                        }
                    })
                    .collect()
            }
            AnySlice::NullableI8(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::I8,
                                value: Some(BytesMut::from(value.to_string().as_bytes())),
                            }
                        } else {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::I8,
                                value: None,
                            }
                        }
                    })
                    .collect()
            }
            AnySlice::NullableI16(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::I16,
                                value: Some(BytesMut::from(value.to_string().as_bytes())),
                            }
                        } else {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::I16,
                                value: None,
                            }
                        }
                    })
                    .collect()
            }
            AnySlice::NullableI32(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::I32,
                                value: Some(BytesMut::from(value.to_string().as_bytes())),
                            }
                        } else {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::I32,
                                value: None,
                            }
                        }
                    })
                    .collect()
            }
            AnySlice::NullableI64(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::I64,
                                value: Some(BytesMut::from(value.to_string().as_bytes())),
                            }
                        } else {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::I64,
                                value: None,
                            }
                        }
                    })
                    .collect()
            }
            AnySlice::NullableU8(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::U8,
                                value: Some(BytesMut::from(vec![*value].as_slice())),
                            }
                        } else {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::U8,
                                value: None,
                            }
                        }
                    })
                    .collect()
            }
            AnySlice::NullableBit(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::Bit,
                                value: Some(BytesMut::from(value.as_bool().to_string().as_bytes())),
                            }
                        } else {
                            OdbcColumnItem {
                                odbc_type: OdbcColumnType::Bit,
                                value: None,
                            }
                        }
                    })
                    .collect()
            }
        }
    }
}

/// Convert `odbc_api::sys::Date` to `time::Date`
///
/// # Example
///
/// ```rust
/// # use time::{Date, macros::date};
/// # use odbc_common::odbc_api::sys::Date as OdbcDate;
/// use odbc_api_helper::TryConvert;
///
/// let odbc_data = OdbcDate{year: 2020,month: 1,day: 1};
/// assert_eq!(date!(2020 - 01 - 01), odbc_data.try_convert().unwrap());
///
/// let odbc_data = OdbcDate{year: 2022,month: 12,day: 31};
/// assert_eq!(date!(2022 - 12 - 31), odbc_data.try_convert().unwrap());
///
/// ```
impl TryConvert<time::Date> for Date {
    type Error = time::Error;

    fn try_convert(self) -> Result<time::Date, Self::Error> {
        Ok(time::Date::from_calendar_date(
            self.year as i32,
            time::Month::try_from(self.month as u8)?,
            self.day as u8,
        )?)
    }
}

/// Convert `odbc_api::sys::Time` to `time::Time`
///
/// # Example
///
/// ```rust
/// # use time::{Date, macros::time};
/// # use odbc_common::odbc_api::sys::Time as OdbcTime;
/// use odbc_api_helper::TryConvert;
///
/// let odbc_time = OdbcTime { hour: 3,minute: 1,second: 1 };
/// assert_eq!(time!(03 : 01: 01), odbc_time.try_convert().unwrap());
///
/// let odbc_time = OdbcTime { hour: 19,minute: 31,second: 59 };
/// assert_eq!(time!(19 : 31 : 59), odbc_time.try_convert().unwrap());
///
/// ```
impl TryConvert<time::Time> for Time {
    type Error = time::Error;
    fn try_convert(self) -> Result<time::Time, Self::Error> {
        Ok(time::Time::from_hms(
            self.hour as u8,
            self.minute as u8,
            self.second as u8,
        )?)
    }
}

impl TryConvert<time::Time> for (Time, u32) {
    type Error = time::Error;
    fn try_convert(self) -> Result<time::Time, Self::Error> {
        let time = self.0;
        let nanosecond = self.1;

        Ok(time::Time::from_hms_nano(
            time.hour as u8,
            time.minute as u8,
            time.second as u8,
            nanosecond,
        )?)
    }
}

impl TryConvert<(time::Date, time::Time)> for Timestamp {
    type Error = time::Error;

    fn try_convert(self) -> Result<(time::Date, time::Time), Self::Error> {
        let date = Date {
            year: self.year,
            month: self.month,
            day: self.day,
        }
        .try_convert()?;
        let time = Time {
            hour: self.hour,
            minute: self.minute,
            second: self.second,
        };
        let nanosecond = self.fraction;
        let time = (time, nanosecond).try_convert()?;
        Ok((date, time))
    }
}

impl TryConvert<time::PrimitiveDateTime> for Timestamp {
    type Error = time::Error;

    fn try_convert(self) -> Result<time::PrimitiveDateTime, Self::Error> {
        let (date, time) = self.try_convert()?;
        Ok(time::PrimitiveDateTime::new(date, time))
    }
}
