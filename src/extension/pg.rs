use bytes::BytesMut;
use postgres_types::{Type as PgType, Oid};
use postgres_protocol::types as pp_type;
use time::{Date, format_description, PrimitiveDateTime, Time};
use crate::Convert;
use crate::extension::odbc::ColumnItem;

pub struct PgColumnItem {
    data: BytesMut,
    pg_type: PgType,
    oid: Oid,
}

impl PgColumnItem {
    fn new(data: BytesMut, pg_type: PgType) -> Self {
        let oid = pg_type.oid();
        Self {
            data,
            pg_type,
            oid,
        }
    }
}


/// referring to link:https://docs.rs/postgres-protocol/0.6.4/postgres_protocol/types/index.html#functions
impl Convert<PgColumnItem> for ColumnItem {
    fn convert(self) -> PgColumnItem {
        let mut buf = BytesMut::new();

        let (_, t) = match self {
            ColumnItem::Text(v) => {
                (v.map(|x| pp_type::text_to_sql(&x, &mut buf)), PgType::TEXT)
            }
            ColumnItem::WText(v) => {
                (v.map(|x| pp_type::text_to_sql(&x, &mut buf)), PgType::TEXT)
            }
            ColumnItem::Binary(v) => {
                (v.map(|x| pp_type::bytea_to_sql(&*x, &mut buf)), PgType::BYTEA)
            }
            ColumnItem::Date(v) => {
                (v.map(|x| {
                    let format = format_description::parse("[year]-[month]-[day]").unwrap();
                    let date = Date::parse(format!("{}-{}-{}", x.year, x.month, x.day).as_str(), &format).unwrap();

                    let base = || -> PrimitiveDateTime {
                        PrimitiveDateTime::new(Date::from_ordinal_date(2000, 1).unwrap(), Time::MIDNIGHT)
                    };
                    let date = (date - base().date()).whole_days();
                    if date > i64::from(i32::max_value()) || date < i64::from(i32::min_value()) {
                        panic!("value too large to transmit");
                    }

                    pp_type::date_to_sql(date as i32, &mut buf);
                }), PgType::DATE)
            }
            ColumnItem::Time(v) => {
                (v.map(|x| {
                    let format = format_description::parse("[hour]:[minute]:[second]").unwrap();
                    let time = Time::parse(format!("{}:{}:{}", x.hour, x.minute, x.second).as_str(), &format).unwrap();
                    let delta = time - Time::MIDNIGHT;
                    let time = i64::try_from(delta.whole_microseconds()).unwrap();
                    pp_type::time_to_sql(time, &mut buf);
                }), PgType::TIME)
            }
            ColumnItem::Timestamp(_) => {
                // (v.map(|x| {
                //     let date = format!("{}:{}:{}", x.hour, x.minute, x.second).parse::<Time>().unwrap();
                //     pp_type::time_to_sql(*date, &mut buf);
                // }), PgType::Timestamp)
                todo!()
            }
            ColumnItem::F64(_) => {
                todo!()
            }
            ColumnItem::F32(_) => {
                todo!()
            }
            ColumnItem::I8(_) => { todo!() }
            ColumnItem::I16(_) => { todo!() }
            ColumnItem::I32(_) => { todo!() }
            ColumnItem::I64(_) => { todo!() }
            ColumnItem::U8(_) => { todo!() }
            ColumnItem::Bit(_) => { todo!() }
        };
        PgColumnItem::new(buf, t)
    }
}