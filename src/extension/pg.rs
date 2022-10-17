use crate::executor::database::Options;
use crate::executor::query::QueryResult;
use crate::executor::statement::SqlValue;
use crate::extension::odbc::{OdbcColumn, OdbcColumnItem, OdbcColumnType};
use crate::{Convert, TryConvert};
use bytes::BytesMut;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use either::Either;
use odbc_api::buffers::BufferKind;
use odbc_api::parameter::InputParameter;
use odbc_api::Bit;
use odbc_api::IntoParameter;
use pg_helper::table::PgTableItem;
use postgres_protocol::types as pp_type;
use postgres_types::{Oid, Type as PgType};
use std::collections::BTreeMap;

use crate::executor::table::TableDescResult;
use crate::executor::SupportDatabase;
use dameng_helper::table::DmTableDesc;
use pg_helper::table::PgTableDesc;

#[derive(Debug)]
pub enum PgValueInput {
    INT2(i16),
    INT4(i32),
    INT8(i64),
    FLOAT4(f32),
    FLOAT8(f64),
    CHAR(String),
    VARCHAR(String),
    TEXT(String),
    Bool(bool),
}

impl SqlValue for PgValueInput {
    fn to_value(&self) -> Either<Box<dyn InputParameter>, ()> {
        macro_rules! left_param {
            ($($arg:tt)*) => {{
                Either::Left(Box::new($($arg)*))
            }};
        }

        match self {
            Self::INT2(i) => left_param!(i.into_parameter()),
            Self::INT4(i) => left_param!(i.into_parameter()),
            Self::INT8(i) => left_param!(i.into_parameter()),
            Self::FLOAT4(i) => left_param!(i.into_parameter()),
            Self::FLOAT8(i) => left_param!(i.into_parameter()),
            Self::CHAR(i) => left_param!(i.to_string().into_parameter()),
            Self::VARCHAR(i) => left_param!(i.to_string().into_parameter()),
            Self::TEXT(i) => left_param!(i.to_string().into_parameter()),
            Self::Bool(i) => left_param!(Bit::from_bool(*i).into_parameter()),
        }
    }
}

#[derive(Debug, Default)]
pub struct PgQueryResult {
    pub columns: Vec<PgColumn>,
    pub data: Vec<Vec<PgColumnItem>>,
}

#[derive(Debug)]
pub struct PgColumn {
    pub name: String,
    pub pg_type: PgType,
    pub oid: Oid,
    pub nullable: bool,
}

#[derive(Debug)]
pub struct PgColumnItem {
    pub data: BytesMut,
    pub pg_type: PgType,
    pub oid: Oid,
}

impl PgColumnItem {
    fn new(data: BytesMut, pg_type: PgType) -> Self {
        let oid = pg_type.oid();
        Self { data, pg_type, oid }
    }
}

impl Convert<PgColumn> for OdbcColumn {
    fn convert(self) -> PgColumn {
        let buffer_kind = BufferKind::from_data_type(self.data_type).unwrap();
        let pg_type = match buffer_kind {
            BufferKind::Binary { .. } => PgType::BYTEA,
            BufferKind::Text { .. } => PgType::TEXT,
            BufferKind::WText { .. } => PgType::TEXT,
            BufferKind::F64 => PgType::FLOAT8,
            BufferKind::F32 => PgType::FLOAT4,
            BufferKind::Date => PgType::DATE,
            BufferKind::Time => PgType::TIME,
            BufferKind::Timestamp => PgType::TIMESTAMP,
            BufferKind::I8 => PgType::CHAR,
            BufferKind::I16 => PgType::INT2,
            BufferKind::I32 => PgType::INT4,
            BufferKind::I64 => PgType::INT8,
            BufferKind::U8 => {
                panic!("not coverage U8");
            }
            BufferKind::Bit => PgType::BOOL,
        };
        let oid = pg_type.oid();
        PgColumn {
            name: self.name,
            pg_type,
            oid,
            nullable: self.nullable,
        }
    }
}

/// referring to link:`<https://docs.rs/postgres-protocol/0.6.4/postgres_protocol/types/index.html#functions>`
impl Convert<PgColumnItem> for OdbcColumnItem {
    fn convert(self) -> PgColumnItem {
        let mut buf = BytesMut::new();

        let (_, t) = match self.odbc_type {
            OdbcColumnType::Text => (
                self.value
                    .map(|x| pp_type::text_to_sql(&String::from_utf8_lossy(x.as_ref()), &mut buf)),
                PgType::TEXT,
            ),

            OdbcColumnType::WText => (
                self.value
                    .map(|x| pp_type::text_to_sql(&String::from_utf8_lossy(x.as_ref()), &mut buf)),
                PgType::TEXT,
            ),

            OdbcColumnType::Binary => (
                self.value
                    .map(|x| pp_type::bytea_to_sql(x.as_ref(), &mut buf)),
                PgType::BYTEA,
            ),
            OdbcColumnType::Date => (
                self.value.map(|x| {
                    let val = String::from_utf8_lossy(x.as_ref()).to_string();
                    let date = NaiveDate::parse_from_str(val.as_str(), "%Y-%m-%d").unwrap();
                    let days = (date - NaiveDate::from_ymd(2000, 1, 1)).num_days();
                    if days > i64::from(i32::max_value()) || days < i64::from(i32::min_value()) {
                        panic!("value too large to transmit");
                    }
                    pp_type::date_to_sql(days as i32, &mut buf)
                }),
                PgType::DATE,
            ),
            OdbcColumnType::Time => (
                self.value.map(|x| {
                    let val = String::from_utf8_lossy(x.as_ref()).to_string();
                    let time = NaiveTime::parse_from_str(val.as_str(), "%H:%M:%S%.f").unwrap();
                    let delta = (time - NaiveTime::from_hms(0, 0, 0))
                        .num_microseconds()
                        .unwrap();
                    pp_type::time_to_sql(delta, &mut buf)
                }),
                PgType::TIME,
            ),
            OdbcColumnType::Timestamp => (
                self.value.map(|x| {
                    let val = String::from_utf8_lossy(x.as_ref()).to_string();
                    let date_time = NaiveDateTime::parse_from_str(
                        val.as_str(),
                        if val.contains('+') {
                            "%Y-%m-%d %H:%M:%S%.f%#z"
                        } else {
                            "%Y-%m-%d %H:%M:%S%.f"
                        },
                    )
                    .unwrap();
                    let epoch = NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0);
                    let ms = (date_time - epoch).num_microseconds().unwrap();
                    pp_type::timestamp_to_sql(ms, &mut buf)
                }),
                PgType::TIMESTAMP,
            ),
            OdbcColumnType::F64 => (
                self.value.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<f64>()
                        .unwrap();
                    pp_type::float8_to_sql(*val, &mut buf)
                }),
                PgType::FLOAT8,
            ),
            OdbcColumnType::F32 => (
                self.value.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<f32>()
                        .unwrap();
                    pp_type::float4_to_sql(*val, &mut buf)
                }),
                PgType::FLOAT4,
            ),
            OdbcColumnType::I8 => (
                self.value.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<i8>()
                        .unwrap();
                    pp_type::char_to_sql(*val, &mut buf)
                }),
                PgType::CHAR,
            ),
            OdbcColumnType::I16 => (
                self.value.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<i16>()
                        .unwrap();
                    pp_type::int2_to_sql(*val, &mut buf)
                }),
                PgType::INT2,
            ),
            OdbcColumnType::I32 => (
                self.value.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<i32>()
                        .unwrap();
                    pp_type::int4_to_sql(*val, &mut buf)
                }),
                PgType::INT4,
            ),
            OdbcColumnType::I64 => (
                self.value.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<i64>()
                        .unwrap();
                    pp_type::int8_to_sql(*val, &mut buf)
                }),
                PgType::INT8,
            ),
            OdbcColumnType::U8 => (
                self.value.map(|x| {
                    let val = x.as_ref().first().unwrap();
                    pp_type::char_to_sql(*val as i8, &mut buf)
                }),
                PgType::CHAR,
            ),
            OdbcColumnType::Bit => (
                self.value.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<bool>()
                        .unwrap();
                    pp_type::bool_to_sql(*val, &mut buf)
                }),
                PgType::BOOL,
            ),
        };
        PgColumnItem::new(buf, t)
    }
}

impl From<QueryResult> for PgQueryResult {
    fn from(result: QueryResult) -> Self {
        PgQueryResult {
            columns: result.columns.into_iter().map(|x| x.convert()).collect(),
            data: result
                .data
                .into_iter()
                .map(|x| x.into_iter().map(|x| x.convert()).collect())
                .collect(),
        }
    }
}

impl Convert<PgType> for Oid {
    fn convert(self) -> PgType {
        PgType::from_oid(self).unwrap()
    }
}

impl Convert<PgType> for PgType {
    fn convert(self) -> PgType {
        self
    }
}

pub fn oid_typlen<C: Convert<PgType>>(c: C) -> i16 {
    let pg_type = c.convert();
    pg_helper::oid_typlen(pg_type)
}

impl TryConvert<PgTableDesc> for (TableDescResult, &Options) {
    type Error = anyhow::Error;

    fn try_convert(self) -> Result<PgTableDesc, Self::Error> {
        let pg = match self.1.database {
            SupportDatabase::Dameng => {
                let dm = DmTableDesc::new(self.0 .0, self.0 .1)?;
                let mut pg = BTreeMap::new();
                for (k, v) in dm.data.into_iter() {
                    let mut pg_item = Vec::new();
                    for dm in v {
                        pg_item.push(dm.try_convert()?)
                    }
                    pg.insert(k.to_string(), pg_item);
                }
                PgTableDesc { data: pg }
            }
            _ => PgTableDesc::default(),
        };

        Ok(pg)
    }
}

impl TryConvert<PgColumnItem> for (&OdbcColumnItem, &PgColumn) {
    type Error = String;

    fn try_convert(self) -> Result<PgColumnItem, Self::Error> {
        let odbc_data = self.0.value.clone();
        let pg_column = self.1;
        let mut buf = BytesMut::new();

        match pg_column.pg_type {
            PgType::TEXT | PgType::VARCHAR => {
                odbc_data
                    .map(|x| pp_type::text_to_sql(&String::from_utf8_lossy(x.as_ref()), &mut buf));
            }

            PgType::BYTEA => {
                odbc_data.map(|x| pp_type::bytea_to_sql(x.as_ref(), &mut buf));
            }

            PgType::DATE => {
                odbc_data.map(|x| {
                    let val = String::from_utf8_lossy(x.as_ref()).to_string();
                    let date = NaiveDate::parse_from_str(val.as_str(), "%Y-%m-%d").unwrap();
                    let days = (date - NaiveDate::from_ymd(2000, 1, 1)).num_days();
                    if days > i64::from(i32::max_value()) || days < i64::from(i32::min_value()) {
                        panic!("value too large to transmit");
                    }
                    pp_type::date_to_sql(days as i32, &mut buf)
                });
            }

            PgType::TIME | PgType::TIMETZ => {
                odbc_data.map(|x| {
                    let val = String::from_utf8_lossy(x.as_ref()).to_string();
                    let time = NaiveTime::parse_from_str(
                        val.as_str(),
                        if val.contains('+') {
                            "%H:%M:%S%.f%#z"
                        } else {
                            "%H:%M:%S%.f"
                        },
                    )
                    .unwrap();
                    let delta = (time - NaiveTime::from_hms(0, 0, 0))
                        .num_microseconds()
                        .unwrap();
                    pp_type::time_to_sql(delta, &mut buf)
                });
            }

            PgType::TIMESTAMP | PgType::TIMESTAMPTZ => {
                odbc_data.map(|x| {
                    let val = String::from_utf8_lossy(x.as_ref()).to_string();
                    let date_time = NaiveDateTime::parse_from_str(
                        val.as_str(),
                        if val.contains('+') {
                            "%Y-%m-%d %H:%M:%S%.f%#z"
                        } else {
                            "%Y-%m-%d %H:%M:%S%.f"
                        },
                    )
                    .unwrap();
                    let epoch = NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0);
                    let ms = (date_time - epoch).num_microseconds().unwrap();
                    pp_type::timestamp_to_sql(ms, &mut buf)
                });
            }
            PgType::FLOAT8 => {
                odbc_data.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<f64>()
                        .unwrap();
                    pp_type::float8_to_sql(*val, &mut buf)
                });
            }
            PgType::FLOAT4 => {
                odbc_data.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<f32>()
                        .unwrap();
                    pp_type::float4_to_sql(*val, &mut buf)
                });
            }
            PgType::CHAR => {
                odbc_data.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<i8>()
                        .unwrap();
                    pp_type::char_to_sql(*val, &mut buf)
                });
            }
            PgType::INT2 => {
                odbc_data.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<i16>()
                        .unwrap();
                    pp_type::int2_to_sql(*val, &mut buf)
                });
            }
            PgType::INT4 | PgType::NUMERIC => {
                odbc_data.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<i32>()
                        .unwrap();
                    pp_type::int4_to_sql(*val, &mut buf)
                });
            }
            PgType::INT8 => {
                odbc_data.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<i64>()
                        .unwrap();
                    pp_type::int8_to_sql(*val, &mut buf)
                });
            }
            PgType::BOOL | PgType::BIT => {
                odbc_data.map(|x| {
                    let val = &String::from_utf8_lossy(x.as_ref())
                        .to_string()
                        .parse::<bool>()
                        .unwrap();
                    pp_type::bool_to_sql(*val, &mut buf)
                });
            }
            _ => {}
        };
        Ok(PgColumnItem::new(buf, pg_column.pg_type.clone()))
    }
}

impl TryConvert<PgQueryResult> for (&QueryResult, &Vec<PgTableItem>, &Options) {
    type Error = String;

    fn try_convert(self) -> Result<PgQueryResult, Self::Error> {
        let res = self.0;
        let pg_all_columns = self.1;
        let option = self.2;

        let mut result = PgQueryResult::default();
        result.columns =
            <(&Vec<OdbcColumn>, &Vec<PgTableItem>) as TryConvert<Vec<PgColumn>>>::try_convert((
                &res.columns,
                &pg_all_columns,
            ))
            .unwrap();

        match option.database {
            crate::executor::SupportDatabase::Dameng => {
                for v in res.data.iter() {
                    let mut row = vec![];
                    for (index, odbc_item) in v.iter().enumerate() {
                        let col = result.columns.get(index).unwrap();
                        row.push(<(&OdbcColumnItem, &PgColumn) as TryConvert<PgColumnItem>>::try_convert((
                                odbc_item,
                                col,
                            )).unwrap()
                        );
                    }
                    result.data.push(row);
                }
            }
            _ => {}
        };
        Ok(result)
    }
}

impl TryConvert<Vec<PgColumn>> for (&Vec<OdbcColumn>, &Vec<PgTableItem>) {
    type Error = String;

    fn try_convert(self) -> Result<Vec<PgColumn>, Self::Error> {
        let odbc_columns = self.0;
        let pg_all_columns = self.1;
        let mut result = vec![];
        for v in odbc_columns.iter() {
            if let Some(pg) = pg_all_columns.iter().find(|&p| p.name == v.name) {
                result.push(PgColumn {
                    name: pg.name.clone(),
                    pg_type: pg.r#type.clone(),
                    oid: pg.r#type.oid(),
                    nullable: pg.nullable,
                });
            }
        }

        Ok(result)
    }
}
