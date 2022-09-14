use crate::Convert;
use odbc_api::buffers::{AnyColumnView, BufferDescription, BufferKind};
use odbc_api::sys::{Date, Time, Timestamp, NULL_DATA};
use odbc_api::DataType;
use std::char::decode_utf16;
use std::ops::Deref;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }
}

impl TryFrom<&Column> for BufferDescription {
    type Error = String;

    fn try_from(c: &Column) -> Result<Self, Self::Error> {
        let description = BufferDescription {
            nullable: c.nullable,
            kind: BufferKind::from_data_type(c.data_type)
                .ok_or_else(|| format!("covert DataType:{:?} to BufferKind error", c.data_type))?,
        };
        Ok(description)
    }
}

#[derive(Debug)]
pub enum ColumnItem {
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
    Unknown(Option<Vec<u8>>)
}

impl ToString for ColumnItem {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}

impl Convert<Vec<ColumnItem>> for AnyColumnView<'_> {
    fn convert(self) -> Vec<ColumnItem> {
        match self {
            AnyColumnView::Text(view) => {
                let mut buffer = Vec::with_capacity(view.len());
                for v in view.iter() {
                    if let Some(x) = v {
                        let cow = String::from_utf8_lossy(x);
                        buffer.push(ColumnItem::Text(Some(cow.to_string())));
                    } else {
                        buffer.push(ColumnItem::Text(None))
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
                        buffer.push(ColumnItem::WText(Some(buf_utf8)));
                    } else {
                        buffer.push(ColumnItem::WText(None))
                    }
                }
                return buffer;
            }
            AnyColumnView::Binary(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    if let Some(bytes) = value {
                        buffer.push(ColumnItem::Binary(Some(bytes.to_vec())))
                    } else {
                        buffer.push(ColumnItem::Binary(None))
                    }
                }
                return buffer;
            }
            AnyColumnView::Date(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnItem::Date(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::Timestamp(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnItem::Timestamp(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::Time(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnItem::Time(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::I32(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnItem::I32(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::Bit(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnItem::Bit(Some(value.as_bool())))
                }
                return buffer;
            }

            AnyColumnView::F64(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnItem::F64(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::F32(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnItem::F32(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::I8(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnItem::I8(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::I16(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnItem::I16(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::I64(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnItem::I64(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::U8(view) => {
                let mut buffer = vec![];
                for value in view.iter() {
                    buffer.push(ColumnItem::U8(Some(*value)))
                }
                return buffer;
            }
            AnyColumnView::NullableDate(_) => {
                warn!("lost NullableDate type");
            }
            AnyColumnView::NullableTime(_) => {
                warn!("lost NullableTime type");
            }
            AnyColumnView::NullableTimestamp(_) => {
                warn!("lost NullableTimestamp type");
            }
            AnyColumnView::NullableF64(_) => {
                warn!("lost NullableF64 type");
            }
            AnyColumnView::NullableF32(_) => {
                warn!("lost NullableF32 type");
            }
            AnyColumnView::NullableI8(_) => {
                warn!("lost NullableI8 type");
            }
            AnyColumnView::NullableI16(_) => {
                warn!("lost NullableI16 type");
            }
            AnyColumnView::NullableI32(view) => {
                let (values, indicators) = view.raw_values();
                let values = values.to_vec();

                return values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if indicators[index] != NULL_DATA {
                            ColumnItem::I32(Some(*value))
                        } else {
                            ColumnItem::I32(None)
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
                            ColumnItem::I64(Some(*value))
                        } else {
                            ColumnItem::I64(None)
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
                            ColumnItem::U8(Some(*value))
                        } else {
                            ColumnItem::U8(None)
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
                            ColumnItem::Bit(Some(value.deref().as_bool()))
                        } else {
                            ColumnItem::Bit(None)
                        }
                    })
                    .collect();
            }
        };
        let opt = self.as_slice::<u8>().map(|x|x.to_vec());
        vec![ColumnItem::Unknown(opt)]
    }
}
