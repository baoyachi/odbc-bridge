use crate::TryConvert;
use dameng_helper::table::DmTableItem;
use dameng_helper::DmDateType;
use pg_helper::table::PgTableItem;
use pg_helper::PgType;

impl TryConvert<PgType> for DmDateType {
    type Error = anyhow::Error;

    fn try_convert(self) -> Result<PgType, Self::Error> {
        match self {
            DmDateType::NUMERIC => Ok(PgType::NUMERIC),
            DmDateType::NUMBER => Ok(PgType::NUMERIC),
            DmDateType::DECIMAL => Ok(PgType::NUMERIC),
            DmDateType::BIT => Ok(PgType::BOOL),
            DmDateType::INTEGER => Ok(PgType::INT4),
            DmDateType::BIGINT => Ok(PgType::INT8),
            DmDateType::TINYINT => Ok(PgType::INT2),
            DmDateType::BYTE => Ok(PgType::INT2),
            DmDateType::SMALLINT => Ok(PgType::INT2),
            DmDateType::BINARY => Ok(PgType::BYTEA),
            DmDateType::VARBINARY => Ok(PgType::VARBIT),
            DmDateType::REAL => Ok(PgType::FLOAT4),
            DmDateType::FLOAT => Ok(PgType::FLOAT4),
            DmDateType::DOUBLE => Ok(PgType::FLOAT4),
            DmDateType::DOUBLE_PRECISION => Ok(PgType::FLOAT8),
            DmDateType::CHAR => Ok(PgType::CHAR),
            DmDateType::VARCHAR => Ok(PgType::VARCHAR),
            DmDateType::TEXT => Ok(PgType::TEXT),
            DmDateType::IMAGE => Ok(PgType::BYTEA),
            DmDateType::BLOB => Ok(PgType::BYTEA),
            DmDateType::CLOB => Ok(PgType::TEXT),
            DmDateType::BFILE => Ok(PgType::BYTEA),
            DmDateType::DATE => Ok(PgType::DATE),
            DmDateType::TIME => Ok(PgType::TIME),
            DmDateType::TIMESTAMP => Ok(PgType::TIMESTAMP),
            DmDateType::TIME_WITH_TIME_ZONE => Ok(PgType::TIMETZ),
            DmDateType::TIMESTAMP_WITH_TIME_ZONE => Ok(PgType::TIMESTAMPTZ),
            DmDateType::TIMESTAMP_WITH_LOCAL_TIME_ZONE => Ok(PgType::TIMESTAMPTZ),
            DmDateType::BOOL => Ok(PgType::BOOL),
            DmDateType::Unknown => Ok(PgType::UNKNOWN),
        }
    }
}

impl TryConvert<PgTableItem> for DmTableItem {
    type Error = anyhow::Error;

    fn try_convert(self) -> Result<PgTableItem, Self::Error> {
        let pg_type: PgType = self.r#type.try_convert()?;
        Ok(PgTableItem {
            name: self.name.to_string(),
            table_id: self.table_id,
            col_index: self.col_index,
            r#type: pg_type,
            length: self.length,
            scale: self.scale,
            nullable: self.nullable,
            default_val: self.default_val,
            table_name: self.table_name,
            create_time: self.create_time,
        })
    }
}
