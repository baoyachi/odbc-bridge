use crate::extension::odbc::ColumnItem;
use crate::Convert;
use bytes::BytesMut;
use postgres_protocol::types as pp_type;
use postgres_types::{Oid, Type as PgType};
use time::{format_description, Date, PrimitiveDateTime, Time};

#[derive(Debug, Default)]
pub struct PgQueryResult {
    pub columns: Vec<PgColumn>,
    pub data: Vec<Vec<PgColumnItem>>,
}

pub struct PgColumn {
    pub name: String,
    pub pg_type: PgType,
    pub oid: Oid,
    pub nullable: bool,
}

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

/// referring to link:https://docs.rs/postgres-protocol/0.6.4/postgres_protocol/types/index.html#functions
impl Convert<PgColumnItem> for ColumnItem {
    fn convert(self) -> PgColumnItem {
        let mut buf = BytesMut::new();

        let (_, t) = match self {
            ColumnItem::Text(v) => (v.map(|x| pp_type::text_to_sql(&x, &mut buf)), PgType::TEXT),
            ColumnItem::WText(v) => (v.map(|x| pp_type::text_to_sql(&x, &mut buf)), PgType::TEXT),
            ColumnItem::Binary(v) => (
                v.map(|x| pp_type::bytea_to_sql(&*x, &mut buf)),
                PgType::BYTEA,
            ),
            ColumnItem::Date(v) => (
                v.map(|x| {
                    let format = format_description::parse("[year]-[month]-[day]").unwrap();
                    let date = Date::parse(
                        format!("{}-{}-{}", x.year, x.month, x.day).as_str(),
                        &format,
                    )
                    .unwrap();

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
            ColumnItem::Time(v) => (
                v.map(|x| {
                    let format = format_description::parse("[hour]:[minute]:[second]").unwrap();
                    let time = Time::parse(
                        format!("{}:{}:{}", x.hour, x.minute, x.second).as_str(),
                        &format,
                    )
                    .unwrap();
                    let delta = time - Time::MIDNIGHT;
                    let time = i64::try_from(delta.whole_microseconds()).unwrap();
                    pp_type::time_to_sql(time, &mut buf);
                }),
                PgType::TIME,
            ),
            ColumnItem::Timestamp(_) => {
                panic!("not coverage Timestamp");
                // (v.map(|x| {
                //     let date = format!("{}:{}:{}", x.hour, x.minute, x.second).parse::<Time>().unwrap();
                //     pp_type::time_to_sql(*date, &mut buf);
                // }), PgType::Timestamp)
            }
            ColumnItem::F64(v) => (
                v.map(|x| pp_type::float8_to_sql(x, &mut buf)),
                PgType::FLOAT8,
            ),
            ColumnItem::F32(v) => (
                v.map(|x| pp_type::float4_to_sql(x, &mut buf)),
                PgType::FLOAT4,
            ),
            ColumnItem::I8(v) => (v.map(|x| pp_type::char_to_sql(x, &mut buf)), PgType::CHAR),
            ColumnItem::I16(v) => (v.map(|x| pp_type::int2_to_sql(x, &mut buf)), PgType::INT2),
            ColumnItem::I32(v) => (v.map(|x| pp_type::int4_to_sql(x, &mut buf)), PgType::INT4),
            ColumnItem::I64(v) => (v.map(|x| pp_type::int8_to_sql(x, &mut buf)), PgType::INT8),
            ColumnItem::U8(_) => {
                panic!("not coverage U8");
            }
            ColumnItem::Bit(v) => (v.map(|x| pp_type::bool_to_sql(x, &mut buf)), PgType::BOOL),
        };
        PgColumnItem::new(buf, t)
    }
}
