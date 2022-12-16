use crate::executor::database::Options;
use crate::executor::query::OdbcRow;
use crate::{Convert, TryConvert};
use bytes::BytesMut;
use odbc_common::error::{OdbcStdError, OdbcStdResult};
use odbc_common::odbc_api::buffers::Indicator;
use odbc_common::odbc_api::handles::{ParameterDescription, Statement, StatementRef};
use odbc_common::odbc_api::parameter::{VarChar, VarCharSliceMut};
use odbc_common::odbc_api::{
    buffers::{AnySlice, BufferDesc},
    sys::{Date, Time, Timestamp, NULL_DATA},
    DataType,
};
use odbc_common::odbc_api::{Bit, ColumnDescription, CursorRow, Nullability, Nullable};
use std::cmp::{max, min};
use std::io::Read;

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

impl TryConvert<BufferDesc> for (&OdbcColumnDesc, &Options) {
    type Error = String;

    fn try_convert(self) -> Result<BufferDesc, Self::Error> {
        let c = self.0;
        let option = self.1;
        let mut desc = BufferDesc::from_data_type(c.data_type, c.nullable)
            .ok_or_else(|| format!("covert DataType:{:?} to BufferDesc error", c.data_type))?;

        // When use `BufferKind::from_data_type` get result with `BufferKind::Text`
        // It's maybe caused panic,it need use `Option.max_str_len` to readjust size.
        // Link: <https://github.com/pacman82/odbc-api/issues/268>
        match desc {
            // TODO Notice: The kind of `BufferDesc::Text` mix up varchar or text type
            // Need to distinguish between text type or varchar type
            BufferDesc::Text { max_str_len } => {
                desc = BufferDesc::WText {
                    max_str_len: min(max_str_len, option.max_str_len),
                };
            }
            BufferDesc::WText { max_str_len } => {
                desc = BufferDesc::WText {
                    max_str_len: min(max_str_len, option.max_str_len),
                };
            }
            BufferDesc::Binary { length } => {
                desc = BufferDesc::Binary {
                    length: min(length, option.max_binary_len),
                };
            }
            _ => {}
        }

        Ok(desc)
    }
}

// https://tools.ietf.org/html/rfc3629
const UTF8_CHAR_WIDTH: &[u8; 256] = &[
    // 1  2  3  4  5  6  7  8  9  A  B  C  D  E  F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 1
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 2
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 3
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 4
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 5
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 6
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 7
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 8
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 9
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // A
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // B
    0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // C
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // D
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // E
    4, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // F
];

/// Given a first byte, determines how many bytes are in this UTF-8 character.
#[must_use]
#[inline]
pub const fn utf8_char_width(b: u8) -> usize {
    UTF8_CHAR_WIDTH[b as usize] as usize
}

pub struct OdbcColsBuf {
    pub columns: Vec<(u16, OdbcDataBuf)>,
}

impl OdbcColsBuf {
    pub fn try_from_col_desc(
        col_desc: &[OdbcColumnDesc],
        max_str_len: usize,
        max_binary_len: usize,
    ) -> OdbcStdResult<Self> {
        let mut columns = Vec::with_capacity(col_desc.len());
        let mut col_num = 0;
        for col_desc in col_desc.iter() {
            let buf =
                OdbcDataBuf::try_from_data_type(col_desc.data_type, max_str_len, max_binary_len)?;
            col_num += 1;
            columns.push((col_num, buf));
        }
        Ok(Self { columns })
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

pub enum OdbcDataBuf {
    Text(Vec<u8>),
    WText(Vec<u8>),
    Binary(Vec<u8>),
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

            OdbcDataBuf::Text(_) | OdbcDataBuf::WText(_) | OdbcDataBuf::Binary(_) => {}
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
                value: Self::get_dameng_long_text(column_number, buf, cursor_row)?,
            },

            OdbcDataBuf::WText(buf) => OdbcColumnItem {
                odbc_type: crate::extension::odbc::OdbcColumnType::WText,
                value: Self::get_dameng_long_text(column_number, buf, cursor_row)?,
            },
            OdbcDataBuf::Binary(buf) => OdbcColumnItem {
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
        let reset_buf = |buffer: &mut [u8]| {
            // buf_length 比 data_length 大1才不会截断.
            // 截断是按照字符而不是字节去截断的, 一个utf8最多占4个字节, 所以可能buf里的倒数第234字节, 总共三个字节没有被使用到(倒数第一个字节总是0).
            // 通过把最后234字节赋值未一个不可能出现在utf8的字节(0xff), 来判断此次获取的数据长度
            if let Some(end) = buffer.rchunks_mut(4).next() {
                for i in end {
                    *i = INVALID_UTF8_BYTE;
                }
            }
            // 通过查看target.is_complete()的判断条件可以知道, buf的最后一个u8必须要是0.
            if let Some(last) = buffer.last_mut() {
                *last = 0;
            }
        };

        let trim_invalid_utf8 = |data_without_zero: &[u8]| -> (bool, usize) {
            let mut current_is_zero = false;
            // 总是移除最后一个字节0
            let mut trim_len = 1;

            if let Some(end) = data_without_zero.rchunks(4).next() {
                let mut iter = end.iter().rev();
                // 不处理最后一个字节0
                iter.next();
                if let Some(second_last) = iter.next() {
                    match *second_last {
                        0 => {
                            current_is_zero = true;
                        }
                        INVALID_UTF8_BYTE => {
                            trim_len += 1;
                            for item in iter {
                                if *item == INVALID_UTF8_BYTE {
                                    trim_len += 1;
                                } else {
                                    // 驱动会在实际返回数据的后一字节设置0, 不一定是buf的最后一个字节, 所以这里还需要移除0
                                    // 比如,(0x61='a') 最开始:
                                    // buf = .. 0xFF 0xFF 0xFF 0x00
                                    // get_data 之后:
                                    // buf = .. 0x61 0x00 0xFF 0x00
                                    // data_without_zero = .. 0x61 0x00 0xFF
                                    // 所以除了移除掉0xFF, 还要移除0x00
                                    trim_len += 1;
                                    break;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        
            (current_is_zero, trim_len)
        };

        let mut actual = Option::<bytes::BytesMut>::None;

        let mut set_data = |v: &[u8], last_time_is_zero: bool| {
            match actual {
                Some(ref mut actual) => {
                    if last_time_is_zero {
                        // 0可能是驱动设置的null-termination, 也可能是字符串本身包含0x00
                        // 上一次最后一个字节是0, 且这一次第一个utf8字符是 一个字节的, 说明上一次最后一个字节0是字符串本身包含的.
                        // 否则这一次的第一个字节本应该放在上一次的最后一个字节处.
                        if utf8_char_width(v[0]) > 1 && actual.len() > 1 {
                            // 移除null-termination
                            actual.truncate(actual.len() - 1);
                        }
                    }
                    actual.extend_from_slice(v);
                }
                None => {
                    actual = Some(bytes::BytesMut::from(v));
                }
            }
        };

        let mut last_time_is_zero = false;

        // Utilize all of the allocated buffer. We must make sure buffer can at least hold the
        // terminating zero. We do a bit more than that though, to avoid to many repeated calls to
        // get_data.
        buf.resize(max(256, buf.capacity()), 0);
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
                    let old_len = buf.len();

                    let (current_is_zero, trim_len) = trim_invalid_utf8(&buf);
                    set_data(&buf[..fetch_size - trim_len], last_time_is_zero);
                    last_time_is_zero = current_is_zero;

                    // Use an exponential strategy for increasing buffer size.
                    buf.resize(old_len * 2, 0);
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
                    set_data(&buf[..buf.len() - shrink_by], last_time_is_zero);
                    break;
                }
                // We did not get all of the value in one go, but the data source has been friendly
                // enough to tell us how much is missing.
                Indicator::Length(len) => {
                    let still_missing = len - fetch_size + 1;
                    let old_len = buf.len();

                    let (current_is_zero, trim_len) = trim_invalid_utf8(&buf);
                    set_data(&buf[..fetch_size - trim_len], last_time_is_zero);
                    last_time_is_zero = current_is_zero;

                    if old_len < still_missing {
                        buf.resize(old_len + still_missing, 0);
                    }
                    fetch_size = buf.len();
                    reset_buf(buf);
                    target = VarCharSliceMut::from_buffer(buf, Indicator::Null);
                    cursor_row.get_data(column_number, &mut target)?;
                }
            }
        }
        Ok(actual)
    }

    pub fn get_long_text(
        column_number: u16,
        buf: &mut [u8],
        cursor_row: &mut CursorRow,
    ) -> OdbcStdResult<Option<BytesMut>> {
        let mut t = Vec::with_capacity(300);

        let r = cursor_row.get_text(column_number, &mut t)?;
        println!("获取到{},{}", t.len(), String::from_utf8_lossy(&t));
        return Ok(r.then(|| bytes::BytesMut::from(t.as_slice())));

        // Indicator::Length(len) 如果截断了, 那么len是截断前的数据库期待的返回长度, 而不是这一次get_data实际返回的长度
        // 如果没有截断, 那么len就是这一次get_data实际返回的长度.

        // 对于达梦, 数据库的字段为34个中文,那通过get_data方法, 理应需要返回102个字节.
        // 如果buf的长度是101字节, 应该要截断
        // 分两次返回:
        //   第一次返回 33个中文*3 = 99byte, 第100位是odbc规定截断的字符串后面要接 0x00(The null-termination character), 而101位不会被驱动更改, 如果原来是1,那get_data之后还是1
        //   第二次返回 1个中文*3 = 3 byte, 除了前3个byte,后续的其他位都不会被修改

        // 如果buf的长度是102字节, 实际还是会被截断. (只有buf长度比data_length大1才不会被截断)
        // 分两次返回:
        //   第一次返回 33个中文*3 = 99byte, 第100位是odbc规定截断的字符串后面要接 0x00(The null-termination character), 而101和102位都不会被驱动更改,
        //   第二次返回 1个中文*3 = 3 byte, 除了前3个byte,后续的其他位都不会被修改

        const INVALID_UTF8_BYTE: u8 = 0xFF;
        let reset_buf = |buffer: &mut [u8]| {
            // buf_length 比 data_length 大1才不会截断.
            // 截断是按照字符而不是字节去截断的, 一个utf8最多占4个字节, 所以可能buf里的倒数第234字节, 总共三个字节没有被使用到(倒数第一个字节总是0).
            // 通过把最后234字节赋值未一个不可能出现在utf8的字节(0xff), 来判断此次获取的数据长度
            if let Some(end) = buffer.rchunks_mut(4).next() {
                for i in end {
                    *i = INVALID_UTF8_BYTE;
                }
            }
            // 通过查看target.is_complete()的判断条件可以知道, buf的最后一个u8必须要是0.
            if let Some(last) = buffer.last_mut() {
                *last = 0;
            }
        };

        let mut actual = Option::<bytes::BytesMut>::None;

        let mut last_time_is_zero = false;
        loop {
            reset_buf(buf);
            let mut target: VarCharSliceMut<'_> = VarChar::from_buffer(buf, Indicator::Null);

            cursor_row.get_data(column_number, &mut target)?;

            if let Some(mut data_without_zero) = target.as_bytes() {
                let mut current_is_zero = false;
                if !target.is_complete() {
                    let mut trim_len = 0;

                    if let Some(end) = data_without_zero.rchunks(3).next() {
                        let mut iter = end.iter().rev();
                        if let Some(last) = iter.next() {
                            match *last {
                                0 => {
                                    current_is_zero = true;
                                }
                                INVALID_UTF8_BYTE => {
                                    trim_len += 1;
                                    for item in iter {
                                        if *item == INVALID_UTF8_BYTE {
                                            trim_len += 1;
                                        } else {
                                            // 驱动会在实际返回数据的后一字节设置0, 不一定是buf的最后一个字节, 所以这里还需要移除0
                                            // 比如,(0x61='a') 最开始:
                                            // buf = .. 0xFF 0xFF 0xFF 0x00
                                            // get_data 之后:
                                            // buf = .. 0x61 0x00 0xFF 0x00
                                            // data_without_zero = .. 0x61 0x00 0xFF
                                            // 所以除了移除掉0xFF, 还要移除0x00
                                            trim_len += 1;
                                            break;
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }

                    data_without_zero = &data_without_zero[..data_without_zero.len() - trim_len]
                }

                match actual {
                    Some(ref mut actual) => {
                        if last_time_is_zero {
                            // 0可能是驱动设置的null-termination, 也可能是字符串本身包含0x00
                            // 上一次最后一个字节是0, 且这一次第一个utf8字符是 一个字节的, 说明上一次最后一个字节0是字符串本身包含的.
                            // 否则这一次的第一个字节本应该放在上一次的最后一个字节处.
                            if utf8_char_width(data_without_zero[0]) > 1 && actual.len() > 1 {
                                // 移除null-termination
                                actual.truncate(actual.len() - 1);
                            }
                        }
                        actual.extend_from_slice(data_without_zero);
                        last_time_is_zero = current_is_zero;
                    }
                    None => {
                        actual = Some(bytes::BytesMut::from(data_without_zero));
                        last_time_is_zero = current_is_zero;
                    }
                }
            }

            if target.is_complete() {
                break;
            }
        }
        Ok(actual)
    }
    pub fn get_long_binary(
        column_number: u16,
        buf: &mut [u8],
        cursor_row: &mut CursorRow,
    ) -> OdbcStdResult<Option<BytesMut>> {
        let mut actual = Option::<bytes::BytesMut>::None;
        let mut target: odbc_common::odbc_api::parameter::VarBinarySliceMut<'_> =
            odbc_common::odbc_api::parameter::VarBinary::from_buffer(buf, Indicator::Null);
        loop {
            cursor_row.get_data(column_number, &mut target)?;
            if let Some(v) = target.as_bytes() {
                match actual {
                    Some(ref mut actual) => {
                        actual.extend_from_slice(v);
                    }
                    None => {
                        actual = Some(bytes::BytesMut::from(v));
                    }
                }
            }

            if target.is_complete() {
                break;
            }
        }
        Ok(actual)
    }

    pub fn try_from_data_type(
        data_type: DataType,
        max_str_len: usize,
        max_binary_len: usize,
    ) -> OdbcStdResult<Self> {
        Self::from_data_type(data_type, max_str_len, max_binary_len).ok_or_else(|| {
            OdbcStdError::StringError(format!(
                "covert DataType:{:?} to OdbcDataBuf error",
                data_type
            ))
        })
    }

    pub fn from_data_type(
        data_type: DataType,
        max_str_len: usize,
        max_binary_len: usize,
    ) -> Option<Self> {
        const UTF8_MAX_BYTE: usize = 4;
        let result = match data_type {
            DataType::Numeric { precision, scale } | DataType::Decimal { precision, scale }
                if scale == 0 && precision < 3 =>
            {
                OdbcDataBuf::I8(Nullable::<i8>::null())
            }
            DataType::Numeric { precision, scale } | DataType::Decimal { precision, scale }
                if scale == 0 && precision < 10 =>
            {
                OdbcDataBuf::I32(Nullable::<i32>::null())
            }
            DataType::Numeric { precision, scale } | DataType::Decimal { precision, scale }
                if scale == 0 && precision < 19 =>
            {
                OdbcDataBuf::I64(Nullable::<i64>::null())
            }
            DataType::Integer => OdbcDataBuf::I32(Nullable::<i32>::null()),
            DataType::SmallInt => OdbcDataBuf::I16(Nullable::<i16>::null()),
            DataType::Float { precision: 0..=24 } | DataType::Real => {
                OdbcDataBuf::F32(Nullable::<f32>::null())
            }
            DataType::Float { precision: 25..=53 } | DataType::Double => {
                OdbcDataBuf::F64(Nullable::<f64>::null())
            }
            DataType::Date => OdbcDataBuf::Date(Nullable::<Date>::null()),
            DataType::Time { precision: 0 } => OdbcDataBuf::Time(Nullable::<Time>::null()),
            DataType::Timestamp { precision: _ } => {
                OdbcDataBuf::Timestamp(Nullable::<Timestamp>::null())
            }
            DataType::BigInt => OdbcDataBuf::I64(Nullable::<i64>::null()),
            DataType::TinyInt => OdbcDataBuf::I8(Nullable::<i8>::null()),
            DataType::Bit => OdbcDataBuf::Bit(Nullable::<Bit>::null()),
            DataType::Varbinary { length }
            | DataType::Binary { length }
            | DataType::LongVarbinary { length } => {
                OdbcDataBuf::Binary(vec![0u8; min(max_binary_len, length)])
            }

            DataType::WVarchar { length } | DataType::WChar { length } => {
                OdbcDataBuf::WText(vec![0u8; max(min(max_str_len, length), UTF8_MAX_BYTE) + 1])
            }

            // Plus one, since the null-termination character.
            DataType::Varchar { length }
            | DataType::Char { length }
            | DataType::LongVarchar { length } => {
                OdbcDataBuf::Text(vec![0u8; max(min(max_str_len, length), UTF8_MAX_BYTE) + 1])
            }
            // Specialized buffers for Numeric and decimal are not yet supported.
            DataType::Numeric {
                precision: _,
                scale: _,
            }
            | DataType::Decimal {
                precision: _,
                scale: _,
            }
            | DataType::Time { precision: _ } => {
                OdbcDataBuf::Text(vec![0u8; data_type.display_size().unwrap()])
            }
            DataType::Unknown
            | DataType::Float { precision: _ }
            | DataType::Other {
                data_type: _,
                column_size: _,
                decimal_digits: _,
            } => return None,
        };
        Some(result)
    }

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
