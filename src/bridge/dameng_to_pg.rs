use pg_helper::PgType;
use dameng_helper::{DataType as DmDateType, DataType};
use crate::TryConvert;

impl TryConvert<PgType> for DmDateType{
    type Error = anyhow::Error;

    fn try_convert(self) -> Result<PgType, Self::Error> {
        match self{
            DataType::NUMERIC => {
                Ok(PgType::NUMERIC)
            }
            DataType::NUMBER => {
                Ok(PgType::NUMERIC)
            }
            DataType::DECIMAL => {
                Ok(PgType::NUMERIC)
            }
            DataType::BIT => {
                Ok(PgType::BIT)
            }
            DataType::INTEGER => {
                Ok(PgType::INT4)
            }
            DataType::BIGINT => {
                Ok(PgType::INT8)
            }
            DataType::TINYINT => {
                Ok(PgType::INT2)
            }
            DataType::BYTE => {
                Ok(PgType::BYTEA)
            }
            DataType::SMALLINT => {
                Ok(PgType::INT2)
            }
            DataType::BINARY => {
                Ok(PgType::BYTEA)
            }
            DataType::VARBINARY => {
                Ok(PgType::VARBIT)
            }
            DataType::REAL => {
                Ok(PgType::FLOAT4)
            }
            DataType::FLOAT => {
                Ok(PgType::FLOAT4)
            }
            DataType::DOUBLE => {
                Ok(PgType::FLOAT4)
            }
            DataType::DOUBLE_PRECISION => {
                Ok(PgType::FLOAT8) 
            }
            DataType::CHAR => {
                Ok(PgType::CHAR)
            }
            DataType::VARCHAR => {
                Ok(PgType::VARCHAR)
            }
            DataType::TEXT => {
                Ok(PgType::TEXT)
            }
            DataType::IMAGE => {
                Ok(PgType::BYTEA)
            }
            DataType::BLOB => {
                Ok(PgType::BYTEA)
            }
            DataType::CLOB => {
                Ok(PgType::TEXT)
            }
            DataType::BFILE => {
                Ok(PgType::BYTEA)
            }
            DataType::DATE => {
                Ok(PgType::DATE)
            }
            DataType::TIME => {
                Ok(PgType::TIME)
            }
            DataType::TIMESTAMP => {
                Ok(PgType::TIMESTAMP)
            }
            DataType::TIME_WITH_TIME_ZONE => {
                Ok(PgType::TIMETZ)
            }
            DataType::TIMESTAMP_WITH_TIME_ZONE => {
                Ok(PgType::TIMESTAMPTZ)
            }
            DataType::TIMESTAMP_WITH_LOCAL_TIME_ZONE => {
                Ok(PgType::TIMESTAMPTZ)
            }
            DataType::BOOL => {
                Ok(PgType::BOOL)
            }
            DataType::Unknown => {
                Ok(PgType::UNKNOWN)
            }
        }
    }
}