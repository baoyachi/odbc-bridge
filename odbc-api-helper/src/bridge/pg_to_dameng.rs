use crate::TryConvert;
use dameng_helper::DmDateType;
use odbc_common::error::{OdbcStdError, OdbcStdResult};
use pg_helper::PgType;

impl TryConvert<DmDateType> for &PgType {
    type Error = OdbcStdError;

    fn try_convert(self) -> OdbcStdResult<DmDateType, Self::Error> {
        match *self {
            PgType::NUMERIC => Ok(DmDateType::NUMERIC),
            PgType::BOOL => Ok(DmDateType::BIT),
            PgType::INT4 => Ok(DmDateType::INTEGER),
            PgType::INT8 => Ok(DmDateType::BIGINT),
            PgType::INT2 => Ok(DmDateType::TINYINT),
            PgType::BYTEA => Ok(DmDateType::BINARY),
            PgType::VARBIT => Ok(DmDateType::VARBINARY),
            PgType::FLOAT4 => Ok(DmDateType::DOUBLE),
            PgType::FLOAT8 => Ok(DmDateType::DOUBLE_PRECISION),
            PgType::CHAR => Ok(DmDateType::CHAR),
            PgType::VARCHAR => Ok(DmDateType::VARCHAR),
            PgType::TEXT => Ok(DmDateType::TEXT),
            PgType::DATE => Ok(DmDateType::DATE),
            PgType::TIME => Ok(DmDateType::TIME),
            PgType::TIMESTAMP => Ok(DmDateType::TIMESTAMP),
            PgType::TIMETZ => Ok(DmDateType::TIME_WITH_TIME_ZONE),
            PgType::TIMESTAMPTZ => Ok(DmDateType::TIMESTAMP_WITH_TIME_ZONE),
            _ => Err(OdbcStdError::TypeConversionError(format!(
                "convert pg data_type to dameng data_type error:{}",
                self
            ))),
        }
    }
}
