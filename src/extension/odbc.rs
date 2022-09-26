use crate::Convert;
use odbc_api::buffers::{AnyColumnView, BufferDescription, BufferKind};
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

impl TryFrom<&OdbcColumn> for BufferDescription {
    type Error = String;

    fn try_from(c: &OdbcColumn) -> Result<Self, Self::Error> {
        let description = BufferDescription {
            nullable: c.nullable,
            kind: BufferKind::from_data_type(c.data_type)
                .ok_or_else(|| format!("covert DataType:{:?} to BufferKind error", c.data_type))?,
        };
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
    Unknown(Option<Vec<u8>>),
}

impl ToString for OdbcColumnItem {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}

impl Convert<Vec<OdbcColumnItem>> for AnyColumnView<'_> {
    fn convert(self) -> Vec<OdbcColumnItem> {
        match self {
            AnyColumnView::Text(view) => {
                let mut buffer = Vec::with_capacity(view.len());
                for v in view.iter() {
                    if let Some(x) = v {
                        let cow = String::from_utf8_lossy(x);
                        buffer.push(OdbcColumnItem::Text(Some(cow.to_string())));
                    } else {
                        buffer.push(OdbcColumnItem::Text(None))
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
                        buffer.push(OdbcColumnItem::WText(Some(buf_utf8)));
                    } else {
                        buffer.push(OdbcColumnItem::WText(None))
                    }
                }
                return buffer;
            }
            AnyColumnView::Binary(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    if let Some(bytes) = value {
                        buffer.push(OdbcColumnItem::Binary(Some(bytes.to_vec())))
                    } else {
                        buffer.push(OdbcColumnItem::Binary(None))
                    }
                }
                return buffer;
            }
            AnyColumnView::Date(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::Date(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::Timestamp(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::Timestamp(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::Time(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::Time(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::I32(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::I32(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::Bit(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::Bit(Some(value.as_bool())))
                }
                return buffer;
            }

            AnyColumnView::F64(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::F64(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::F32(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::F32(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::I8(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::I8(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::I16(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::I16(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::I64(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::I64(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::U8(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(OdbcColumnItem::U8(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::NullableDate(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                return values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem::Date(Some(*value))
                        } else {
                            OdbcColumnItem::Date(None)
                        }
                    })
                    .collect();
            }
            AnyColumnView::NullableTime(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                return values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem::Time(Some(*value))
                        } else {
                            OdbcColumnItem::Time(None)
                        }
                    })
                    .collect();
            }
            AnyColumnView::NullableTimestamp(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                return values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem::Timestamp(Some(*value))
                        } else {
                            OdbcColumnItem::Timestamp(None)
                        }
                    })
                    .collect();
            }
            AnyColumnView::NullableF64(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                return values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem::F64(Some(*value))
                        } else {
                            OdbcColumnItem::F64(None)
                        }
                    })
                    .collect();
            }
            AnyColumnView::NullableF32(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                return values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem::F32(Some(*value))
                        } else {
                            OdbcColumnItem::F32(None)
                        }
                    })
                    .collect();
            }
            AnyColumnView::NullableI8(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                return values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem::I8(Some(*value))
                        } else {
                            OdbcColumnItem::I8(None)
                        }
                    })
                    .collect();
            }
            AnyColumnView::NullableI16(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                return values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem::I16(Some(*value))
                        } else {
                            OdbcColumnItem::I16(None)
                        }
                    })
                    .collect();
            }
            AnyColumnView::NullableI32(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                return values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem::I32(Some(*value))
                        } else {
                            OdbcColumnItem::I32(None)
                        }
                    })
                    .collect();
            }
            AnyColumnView::NullableI64(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                return values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem::I64(Some(*value))
                        } else {
                            OdbcColumnItem::I64(None)
                        }
                    })
                    .collect();
            }
            AnyColumnView::NullableU8(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                return values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem::U8(Some(*value))
                        } else {
                            OdbcColumnItem::U8(None)
                        }
                    })
                    .collect();
            }
            AnyColumnView::NullableBit(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                return values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            OdbcColumnItem::Bit(Some(value.deref().as_bool()))
                        } else {
                            OdbcColumnItem::Bit(None)
                        }
                    })
                    .collect();
            }
        };
    }
}
