use pg_helper::PgType;
use dameng_helper::{DataType as DmDateType, DataType};
use crate::TryConvert;

impl TryConvert<PgType> for DmDateType{
    type Error = anyhow::Error;

    fn try_convert(self) -> Result<PgType, Self::Error> {
        //TODO to be completed
        match self{
            DataType::NUMERIC => {}
            DataType::NUMBER => {}
            DataType::DECIMAL => {}
            DataType::BIT => {}
            DataType::INTEGER => {}
            DataType::BIGINT => {}
            DataType::TINYINT => {}
            DataType::BYTE => {}
            DataType::SMALLINT => {}
            DataType::BINARY => {}
            DataType::VARBINARY => {}
            DataType::REAL => {}
            DataType::FLOAT => {}
            DataType::DOUBLE => {}
            DataType::DOUBLE_PRECISION => {}
            DataType::CHAR => {}
            DataType::VARCHAR => {}
            DataType::TEXT => {}
            DataType::IMAGE => {}
            DataType::BLOB => {}
            DataType::CLOB => {}
            DataType::BFILE => {}
            DataType::DATE => {}
            DataType::TIME => {}
            DataType::TIMESTAMP => {}
            DataType::TIME_WITH_TIME_ZONE => {}
            DataType::TIMESTAMP_WITH_TIME_ZONE => {}
            DataType::TIMESTAMP_WITH_LOCAL_TIME_ZONE => {}
            DataType::BOOL => {}
            DataType::Unknown => {}
        }
        todo!()
    }
}