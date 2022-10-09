use crate::executor::query::QueryResult;
use crate::executor::statement::SqlValue;
use crate::extension::odbc::{OdbcColumn, OdbcColumnItem};
use crate::{Convert, TryConvert};
use bytes::BytesMut;
use either::Either;
use odbc_api::buffers::BufferKind;
use odbc_api::parameter::InputParameter;
use odbc_api::Bit;
use odbc_api::IntoParameter;
use postgres_protocol::types as pp_type;
use postgres_types::{Oid, Type as PgType};

use time::{Date, PrimitiveDateTime, Time};

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

        let (_, t) = match self {
            OdbcColumnItem::Text(v) => {
                (v.map(|x| pp_type::text_to_sql(&x, &mut buf)), PgType::TEXT)
            }
            OdbcColumnItem::WText(v) => {
                (v.map(|x| pp_type::text_to_sql(&x, &mut buf)), PgType::TEXT)
            }
            OdbcColumnItem::Binary(v) => (
                v.map(|x| pp_type::bytea_to_sql(&*x, &mut buf)),
                PgType::BYTEA,
            ),
            OdbcColumnItem::Date(v) => (
                v.map(|x| {
                    let date = x.try_convert().unwrap();

                    let base = || -> PrimitiveDateTime {
                        PrimitiveDateTime::new(
                            Date::from_ordinal_date(2000, 1).unwrap(),
                            Time::MIDNIGHT,
                        )
                    };
                    let date = (date - base().date()).whole_days();
                    if date > i64::from(i32::max_value()) || date < i64::from(i32::min_value()) {
                        panic!("value too large to transmit");
                    }
                    pp_type::date_to_sql(date as i32, &mut buf);
                }),
                PgType::DATE,
            ),
            OdbcColumnItem::Time(v) => (
                v.map(|x| {
                    let time: Time = x.try_convert().unwrap();

                    let delta = time - Time::MIDNIGHT;
                    let time = i64::try_from(delta.whole_microseconds()).unwrap();
                    pp_type::time_to_sql(time, &mut buf);
                }),
                PgType::TIME,
            ),
            OdbcColumnItem::Timestamp(v) => (
                v.map(|x| {
                    let base = || -> PrimitiveDateTime {
                        PrimitiveDateTime::new(
                            Date::from_ordinal_date(2000, 1).unwrap(),
                            Time::MIDNIGHT,
                        )
                    };

                    let date_time: PrimitiveDateTime = x.try_convert().unwrap();
                    let time = i64::try_from((date_time - base()).whole_microseconds()).unwrap();
                    pp_type::timestamp_to_sql(time, &mut buf);
                }),
                PgType::TIMESTAMP,
            ),
            OdbcColumnItem::F64(v) => (
                v.map(|x| pp_type::float8_to_sql(x, &mut buf)),
                PgType::FLOAT8,
            ),
            OdbcColumnItem::F32(v) => (
                v.map(|x| pp_type::float4_to_sql(x, &mut buf)),
                PgType::FLOAT4,
            ),
            OdbcColumnItem::I8(v) => (v.map(|x| pp_type::char_to_sql(x, &mut buf)), PgType::CHAR),
            OdbcColumnItem::I16(v) => (v.map(|x| pp_type::int2_to_sql(x, &mut buf)), PgType::INT2),
            OdbcColumnItem::I32(v) => (v.map(|x| pp_type::int4_to_sql(x, &mut buf)), PgType::INT4),
            OdbcColumnItem::I64(v) => (v.map(|x| pp_type::int8_to_sql(x, &mut buf)), PgType::INT8),
            OdbcColumnItem::U8(v) => (
                v.map(|x| pp_type::char_to_sql(x as i8, &mut buf)),
                PgType::CHAR,
            ),
            OdbcColumnItem::Bit(v) => (v.map(|x| pp_type::bool_to_sql(x, &mut buf)), PgType::BOOL),
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
