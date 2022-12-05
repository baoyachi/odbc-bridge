use crate::executor::database::Options;
use crate::executor::query::QueryResult;
use crate::executor::statement::SqlValue;
use crate::extension::odbc::{OdbcColumnDesc, OdbcColumnItem, OdbcColumnType};
use crate::{Convert, TryConvert};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use dameng_helper::odbc_api::DataType;
use either::Either;
use odbc_common::error::{OdbcStdError, OdbcStdResult};
use odbc_common::odbc_api::buffers::BufferDesc;
use odbc_common::odbc_api::parameter::InputParameter;
use odbc_common::odbc_api::Bit;
use odbc_common::odbc_api::IntoParameter;
use pg_helper::table::PgTableItem;
use postgres_types::{Oid, Type as PgType};
use std::any::Any;
use std::collections::BTreeMap;

use crate::executor::table::TableDescResult;
use crate::executor::SupportDatabase;
use crate::extension::util::{
    parse_to_bool, parse_to_data_time, parse_to_date, parse_to_float4, parse_to_float8,
    parse_to_i8, parse_to_int2, parse_to_int4, parse_to_int8, parse_to_string, parse_to_time,
};
use dameng_helper::table::DmTableDesc;
use pg_helper::table::PgTableDesc;

#[derive(Debug, PartialEq)]
pub enum PgValueInput {
    Int2(Option<i16>),
    Int4(Option<i32>),
    Int8(Option<i64>),
    Float4(Option<f32>),
    Float8(Option<f64>),
    Char(Option<i8>),
    Varchar(Option<String>),
    Text(Option<String>),
    Bool(Option<bool>),
    Bytea(Option<Vec<u8>>),
    Time(Option<NaiveTime>),
    Timez(Option<NaiveTime>),
    Timestamp(Option<NaiveDateTime>),
    Timestampz(Option<NaiveDateTime>),
    Date(Option<NaiveDate>),
    Numeric(Option<i32>),
    Name(Option<String>),
}

impl SqlValue for PgValueInput {
    fn to_value(self) -> Either<Box<dyn InputParameter>, Box<dyn Any>> {
        macro_rules! left_param {
            ($($arg:tt)*) => {{
                Either::Left(Box::new($($arg)*))
            }};
        }

        match self {
            Self::Int2(i) => left_param!(i.into_parameter()),
            Self::Int4(i) | Self::Numeric(i) => left_param!(i.into_parameter()),
            Self::Int8(i) => left_param!(i.into_parameter()),
            Self::Float4(i) => left_param!(i.into_parameter()),
            Self::Float8(i) => left_param!(i.into_parameter()),
            Self::Char(i) => left_param!(i.into_parameter()),
            Self::Varchar(i) | Self::Text(i) | Self::Name(i) => left_param!(i.into_parameter()),
            Self::Bool(i) => left_param!(i.map(Bit::from_bool).into_parameter()),
            Self::Bytea(bytes) => left_param!(bytes.into_parameter()),
            Self::Time(i) | Self::Timez(i) => {
                left_param!(i.map(|i| i.to_string()).into_parameter())
            }
            Self::Timestamp(i) | Self::Timestampz(i) => {
                left_param!(i.map(|i| i.to_string()).into_parameter())
            }
            Self::Date(i) => left_param!(i.map(|i| i.to_string()).into_parameter()),
        }
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct PgQueryResult {
    pub columns: Vec<PgColumn>,
    pub data: Vec<Vec<PgColumnItem>>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PgColumn {
    pub name: String,
    pub pg_type: PgType,
    pub oid: Oid,
    pub nullable: bool,
}

#[derive(Debug, PartialEq)]
pub struct PgColumnItem {
    pub data: Option<PgValueInput>,
}

impl PgColumnItem {
    fn new(data: Option<PgValueInput>) -> Self {
        Self { data }
    }
}

impl Convert<PgColumn> for OdbcColumnDesc {
    fn convert(self) -> PgColumn {
        let pg_type: PgType = (self.data_type, self.nullable).convert();
        let oid = pg_type.oid();
        PgColumn {
            name: self.name,
            pg_type,
            oid,
            nullable: self.nullable,
        }
    }
}

impl Convert<PgType> for (DataType, bool) {
    fn convert(self) -> PgType {
        let desc = BufferDesc::from_data_type(self.0, self.1).unwrap();
        match desc {
            BufferDesc::Binary { .. } => PgType::BYTEA,
            BufferDesc::Text { .. } => PgType::TEXT,
            BufferDesc::WText { .. } => PgType::TEXT,
            BufferDesc::F64 { .. } => PgType::FLOAT8,
            BufferDesc::F32 { .. } => PgType::FLOAT4,
            BufferDesc::Date { .. } => PgType::DATE,
            BufferDesc::Time { .. } => PgType::TIME,
            BufferDesc::Timestamp { .. } => PgType::TIMESTAMP,
            BufferDesc::I8 { .. } => PgType::CHAR,
            BufferDesc::I16 { .. } => PgType::INT2,
            BufferDesc::I32 { .. } => PgType::INT4,
            BufferDesc::I64 { .. } => PgType::INT8,
            BufferDesc::U8 { .. } => {
                panic!("not coverage U8");
            }
            BufferDesc::Bit { .. } => PgType::BOOL,
        }
    }
}

/// referring to link:`<https://docs.rs/postgres-protocol/0.6.4/postgres_protocol/types/index.html#functions>`
impl Convert<PgColumnItem> for OdbcColumnItem {
    fn convert(self) -> PgColumnItem {
        let value = match self.odbc_type {
            OdbcColumnType::Text | OdbcColumnType::WText => self
                .value
                .map(|x| PgValueInput::Text(Some(parse_to_string(x)))),
            OdbcColumnType::Binary => self.value.map(|x| PgValueInput::Bytea(Some(x.to_vec()))),
            OdbcColumnType::Date => self
                .value
                .map(|x| PgValueInput::Date(Some(parse_to_date(x).unwrap()))),
            OdbcColumnType::Time => self
                .value
                .map(|x| PgValueInput::Time(Some(parse_to_time(x).unwrap()))),
            OdbcColumnType::Timestamp => self
                .value
                .map(|x| PgValueInput::Timestamp(Some(parse_to_data_time(x).unwrap()))),
            OdbcColumnType::F64 => self
                .value
                .map(|x| PgValueInput::Float8(Some(parse_to_float8(x).unwrap()))),
            OdbcColumnType::F32 => self
                .value
                .map(|x| PgValueInput::Float4(Some(parse_to_float4(x).unwrap()))),
            OdbcColumnType::I8 | OdbcColumnType::U8 => self
                .value
                .map(|x| PgValueInput::Char(Some(parse_to_i8(x).unwrap()))),
            OdbcColumnType::I16 => self
                .value
                .map(|x| PgValueInput::Int2(Some(parse_to_int2(x).unwrap()))),
            OdbcColumnType::I32 => self
                .value
                .map(|x| PgValueInput::Int4(Some(parse_to_int4(x).unwrap()))),
            OdbcColumnType::I64 => self
                .value
                .map(|x| PgValueInput::Int8(Some(parse_to_int8(x).unwrap()))),
            OdbcColumnType::Bit => self
                .value
                .map(|x| PgValueInput::Bool(Some(parse_to_bool(x).unwrap()))),
        };
        PgColumnItem::new(value)
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
    type Error = OdbcStdError;

    fn try_convert(self) -> OdbcStdResult<PgTableDesc, Self::Error> {
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
    type Error = OdbcStdError;

    fn try_convert(self) -> OdbcStdResult<PgColumnItem, Self::Error> {
        let odbc_data = self.0.value.clone();
        let pg_column = self.1;

        let value = match pg_column.pg_type {
            PgType::TEXT => odbc_data.map(|v| PgValueInput::Text(Some(parse_to_string(v)))),
            PgType::VARCHAR => odbc_data.map(|v| PgValueInput::Varchar(Some(parse_to_string(v)))),
            PgType::BYTEA => odbc_data.map(|v| PgValueInput::Bytea(Some(v.to_vec()))),
            PgType::DATE => odbc_data.map(|v| PgValueInput::Date(Some(parse_to_date(v).unwrap()))),
            PgType::TIME => odbc_data.map(|v| PgValueInput::Time(Some(parse_to_time(v).unwrap()))),
            PgType::TIMETZ => {
                odbc_data.map(|v| PgValueInput::Timez(Some(parse_to_time(v).unwrap())))
            }
            PgType::TIMESTAMP => {
                odbc_data.map(|v| PgValueInput::Timestamp(Some(parse_to_data_time(v).unwrap())))
            }
            PgType::TIMESTAMPTZ => {
                odbc_data.map(|v| PgValueInput::Timestampz(Some(parse_to_data_time(v).unwrap())))
            }
            PgType::FLOAT8 => {
                odbc_data.map(|v| PgValueInput::Float8(Some(parse_to_float8(v).unwrap())))
            }
            PgType::FLOAT4 => {
                odbc_data.map(|v| PgValueInput::Float4(Some(parse_to_float4(v).unwrap())))
            }
            PgType::CHAR => odbc_data.map(|v| PgValueInput::Char(Some(parse_to_i8(v).unwrap()))),
            PgType::INT2 => odbc_data.map(|v| PgValueInput::Int2(Some(parse_to_int2(v).unwrap()))),
            PgType::INT4 => odbc_data.map(|v| PgValueInput::Int4(Some(parse_to_int4(v).unwrap()))),
            PgType::NUMERIC => {
                odbc_data.map(|v| PgValueInput::Numeric(Some(parse_to_int4(v).unwrap())))
            }
            PgType::INT8 => odbc_data.map(|v| PgValueInput::Int8(Some(parse_to_int8(v).unwrap()))),
            PgType::BOOL => odbc_data.map(|v| PgValueInput::Bool(Some(parse_to_bool(v).unwrap()))),
            _ => {
                error!(
                    "There is no adaptation for this type, {}",
                    pg_column.pg_type
                );
                odbc_data.map(|v| PgValueInput::Text(Some(parse_to_string(v))))
            }
        };

        Ok(PgColumnItem::new(value))
    }
}

impl TryConvert<PgQueryResult> for (QueryResult, &Vec<PgTableItem>, &Options) {
    type Error = OdbcStdError;

    fn try_convert(self) -> OdbcStdResult<PgQueryResult, Self::Error> {
        let res = self.0;
        let pg_all_columns = self.1;
        let options = self.2;
        let mut result = PgQueryResult::default();
        if let Ok(cols) = (&res.columns, pg_all_columns, options).try_convert() {
            let cols: Vec<PgColumn> = cols;
            result.columns = cols;

            // if column name is count(*),but this name not exist Vec<PgTableItem>
            // So,could find result.columns is empty.
            if result.columns.is_empty() {
                return Ok(PgQueryResult::from(res));
            }

            if let crate::executor::SupportDatabase::Dameng = options.database {
                for v in res.data.iter() {
                    let mut row: Vec<PgColumnItem> = vec![];
                    for (index, odbc_item) in v.iter().enumerate() {
                        if let Some(col) = result.columns.get(index) {
                            row.push((odbc_item, col).try_convert().unwrap());
                        }
                    }
                    result.data.push(row);
                }
            }
        }
        Ok(result)
    }
}

impl TryConvert<Vec<PgColumn>> for (&Vec<OdbcColumnDesc>, &Vec<PgTableItem>, &Options) {
    type Error = OdbcStdError;

    fn try_convert(self) -> OdbcStdResult<Vec<PgColumn>, Self::Error> {
        let odbc_columns = self.0;
        let pg_all_columns = self.1;
        let options = self.2;
        let mut result = vec![];
        for v in odbc_columns.iter() {
            let find_name = |source: &str, target: &str| -> bool {
                if options.case_sensitive {
                    source == target
                } else {
                    source.to_uppercase() == target.to_uppercase()
                }
            };

            if let Some(pg) = pg_all_columns.iter().find(|&p| find_name(&p.name, &v.name)) {
                result.push(PgColumn {
                    name: pg.name.clone(),
                    pg_type: pg.r#type.clone(),
                    oid: pg.r#type.oid(),
                    nullable: pg.nullable,
                });
            } else {
                result.push(v.clone().convert());
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use odbc_common::odbc_api::DataType;

    #[test]
    fn test_query_result_convert() {
        let column = OdbcColumnDesc {
            name: "trace_id".to_string(),
            data_type: DataType::Varchar { length: 255 },
            nullable: true,
        };

        let query_result = QueryResult {
            columns: vec![column],
            data: vec![vec![OdbcColumnItem {
                odbc_type: OdbcColumnType::Text,
                value: None,
            }]],
        };

        let pg_table_item = PgTableItem {
            name: "trace_id".to_string(),
            table_id: 0,
            col_index: 0,
            r#type: PgType::VARCHAR,
            length: 255,
            scale: 0,
            nullable: true,
            is_identity: false,
            default_val: None,
            table_name: "".to_string(),
            create_time: "".to_string(),
            subtype: None,
        };
        let options = Options {
            database: SupportDatabase::Dameng,
            max_batch_size: 1024,
            max_str_len: 1024,
            max_binary_len: 1024,
            case_sensitive: false,
        };
        let result: PgQueryResult = (query_result, &vec![pg_table_item], &options)
            .try_convert()
            .unwrap();
        assert_eq!(
            result,
            PgQueryResult {
                columns: vec![PgColumn {
                    name: "trace_id".to_string(),
                    pg_type: PgType::VARCHAR,
                    oid: 1043,
                    nullable: true,
                }],
                data: vec![vec![PgColumnItem { data: None }]],
            }
        );
    }
}
