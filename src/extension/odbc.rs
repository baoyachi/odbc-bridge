use crate::executor::database::Options;
use crate::{Convert, TryConvert};
use odbc_api::buffers::{AnySlice, BufferDescription, BufferKind};
use odbc_api::sys::{Date, Time, Timestamp, NULL_DATA};
use odbc_api::DataType;
use std::char::decode_utf16;
use std::ops::Deref;

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
        // Link: <https://github.com/baoyachi/odbc-api-helper/issues/35>
        match description.kind {
            BufferKind::Text { .. } => {
                description.kind = BufferKind::Text {
                    max_str_len: option.max_str_len,
                };
            }
            _ => {}
        }

        Ok(description)
    }
}

#[derive(Debug)]
pub enum OdbcColumnItem {
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
                        let cow = String::from_utf8_lossy(x);
                        buffer.push(OdbcColumnItem::Text(Some(cow.to_string())));
                    } else {
                        buffer.push(OdbcColumnItem::Text(None))
                    }
                }
                buffer
            }
            AnySlice::WText(view) => {
                let mut buffer = Vec::with_capacity(view.len());
                for value in view.iter() {
                    if let Some(utf16) = value {
                        let mut buf_utf8 = String::new();
                        for c in decode_utf16(utf16.as_slice().iter().cloned()) {
                            buf_utf8.push(c.unwrap());
                        }
                        buffer.push(OdbcColumnItem::WText(Some(buf_utf8)));
                    } else {
                        buffer.push(OdbcColumnItem::WText(None))
                    }
                }
                buffer
            }
            AnySlice::Binary(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    if let Some(bytes) = value {
                        buffer.push(OdbcColumnItem::Binary(Some(bytes.to_vec())))
                    } else {
                        buffer.push(OdbcColumnItem::Binary(None))
                    }
                }
                buffer
            }
            AnySlice::Date(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::Date(Some(*value)))
                }
                buffer
            }
            AnySlice::Timestamp(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::Timestamp(Some(*value)))
                }
                buffer
            }
            AnySlice::Time(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::Time(Some(*value)))
                }
                buffer
            }
            AnySlice::I32(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::I32(Some(*value)))
                }
                buffer
            }
            AnySlice::Bit(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::Bit(Some(value.as_bool())))
                }
                buffer
            }

            AnySlice::F64(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::F64(Some(*value)))
                }
                buffer
            }
            AnySlice::F32(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::F32(Some(*value)))
                }
                buffer
            }
            AnySlice::I8(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::I8(Some(*value)))
                }
                buffer
            }
            AnySlice::I16(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::I16(Some(*value)))
                }
                buffer
            }
            AnySlice::I64(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::I64(Some(*value)))
                }
                buffer
            }
            AnySlice::U8(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::U8(Some(*value)))
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
                            OdbcColumnItem::Date(Some(*value))
                        } else {
                            OdbcColumnItem::Date(None)
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
                            OdbcColumnItem::Time(Some(*value))
                        } else {
                            OdbcColumnItem::Time(None)
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
                            OdbcColumnItem::Timestamp(Some(*value))
                        } else {
                            OdbcColumnItem::Timestamp(None)
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
                            OdbcColumnItem::F64(Some(*value))
                        } else {
                            OdbcColumnItem::F64(None)
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
                            OdbcColumnItem::F32(Some(*value))
                        } else {
                            OdbcColumnItem::F32(None)
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
                            OdbcColumnItem::I8(Some(*value))
                        } else {
                            OdbcColumnItem::I8(None)
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
                            OdbcColumnItem::I16(Some(*value))
                        } else {
                            OdbcColumnItem::I16(None)
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
                            OdbcColumnItem::I32(Some(*value))
                        } else {
                            OdbcColumnItem::I32(None)
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
                            OdbcColumnItem::I64(Some(*value))
                        } else {
                            OdbcColumnItem::I64(None)
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
                            OdbcColumnItem::U8(Some(*value))
                        } else {
                            OdbcColumnItem::U8(None)
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
                            OdbcColumnItem::Bit(Some(value.deref().as_bool()))
                        } else {
                            OdbcColumnItem::Bit(None)
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
        let nanosecond = self.fraction as u32;
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
