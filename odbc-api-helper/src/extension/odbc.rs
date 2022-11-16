use crate::executor::database::Options;
use crate::{Convert, TryConvert};
use bytes::BytesMut;
use odbc_api::buffers::{AnySlice, BufferDescription, BufferKind};
use odbc_api::sys::{Date, Time, Timestamp, NULL_DATA};
use odbc_api::DataType;
use std::cmp::min;

#[derive(Debug, Clone)]
pub struct OdbcColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl OdbcColumn {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }
}

impl TryConvert<BufferDescription> for (&OdbcColumn, &Options) {
    type Error = String;

    fn try_convert(self) -> Result<BufferDescription, Self::Error> {
        let c = self.0;
        let option = self.1;
        let mut description = BufferDescription {
            nullable: c.nullable,
            kind: BufferKind::from_data_type(c.data_type)
                .ok_or_else(|| format!("covert DataType:{:?} to BufferKind error", c.data_type))?,
        };

        // When use `BufferKind::from_data_type` get result with `BufferKind::Text`
        // It's maybe caused panic,it need use `Option.max_str_len` to readjust size.
        // Link: <https://github.com/pacman82/odbc-api/issues/268>
        match description.kind {
            // TODO Notice: The kind of `BufferKind::Text` mix up varchar or text type
            // Need to distinguish between text type or varchar type
            BufferKind::Text { max_str_len } => {
                description.kind = BufferKind::Text {
                    max_str_len: min(max_str_len, option.max_str_len),
                };
            }
            BufferKind::WText { max_str_len } => {
                description.kind = BufferKind::WText {
                    max_str_len: min(max_str_len, option.max_str_len),
                };
            }
            BufferKind::Binary { length } => {
                description.kind = BufferKind::Binary {
                    length: min(length, option.max_binary_len),
                }
            }
            _ => {}
        }

        Ok(description)
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
        format!("{:?}", self)
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
/// # use odbc_api::sys::Date as OdbcDate;
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
/// # use odbc_api::sys::Time as OdbcTime;
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
