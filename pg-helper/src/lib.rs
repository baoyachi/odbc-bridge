#![deny(missing_debug_implementations)]
#![allow(dead_code)]

#[macro_use]
extern crate serde;

mod parser;
pub mod state;
pub mod table;
pub use postgres_types::Type as PgType;

pub fn oid_typlen(pg_type: PgType) -> i16 {
    match pg_type {
        PgType::BOOL => 1,
        PgType::BYTEA => -1,
        PgType::CHAR => 1,
        PgType::INT8 => 8,
        PgType::INT2 => 2,
        PgType::INT2_VECTOR => -1,
        PgType::INT4 => 4,
        PgType::TEXT => -1,
        PgType::FLOAT4 => 4,
        PgType::FLOAT8 => 8,
        PgType::VARCHAR => -1,
        PgType::DATE => 4,
        PgType::TIME => 8,
        PgType::TIMESTAMP => 8,
        PgType::TIMESTAMPTZ => 8,
        PgType::TIMETZ => 12,
        PgType::BIT => -1,
        PgType::JSONB => -1,
        PgType::NAME => 64,
        _ => panic!("unknown pg_type:{}", pg_type),
    }
}
