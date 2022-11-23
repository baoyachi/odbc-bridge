use crate::DmDateType;
use odbc_common::{Print, StyledString, Table, TableTheme, TextStyle};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;
use strum::{Display, EnumString};

/// dameng database table item
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DmTableItem {
    pub name: String,
    pub table_id: usize,
    pub col_index: usize,
    pub r#type: DmDateType,
    pub length: usize,
    pub scale: usize,
    pub nullable: bool,
    pub is_identity: bool,
    pub default_val: Option<String>,
    pub table_name: String,
    pub create_time: String,
    pub subtype: Option<String>,
}

impl DmTableItem {
    fn to_vec(&self) -> Vec<String> {
        let mut vec = vec![];
        vec.push(self.name.to_string());
        vec.push(self.table_id.to_string());
        vec.push(self.col_index.to_string());
        vec.push(format!("{:?}", self.r#type));
        vec.push(self.length.to_string());
        vec.push(self.scale.to_string());
        vec.push(self.nullable.to_string());
        vec.push(self.default_val.clone().unwrap_or_default());
        vec.push(self.table_name.to_string());
        vec.push(self.create_time.to_string());
        vec.push(self.subtype.clone().unwrap_or_default());
        vec
    }
}

/// table describe
#[derive(Debug, EnumString, Display, Serialize, Deserialize)]
pub enum ColNameEnum {
    #[strum(to_string = "NAME")]
    Name,
    #[strum(to_string = "ID")]
    Id,
    #[strum(to_string = "COLID")]
    ColId,
    #[strum(to_string = "TYPE$")]
    Type,
    #[strum(to_string = "LENGTH$")]
    Length,
    #[strum(to_string = "SCALE")]
    Scale,
    #[strum(to_string = "NULLABLE$")]
    Nullable,
    #[strum(to_string = "IS_IDENTITY")]
    IsIdentity,
    #[strum(to_string = "DEFVAL")]
    DefaultVal,
    #[strum(to_string = "TABLE_NAME")]
    TableName,
    #[strum(to_string = "CRTDATE")]
    CreateTime,
    #[strum(to_string = "SUBTYPE$")]
    SubType,
}

/// The table data. Execute sql get table describe
/// ```bash
/// > SELECT A.NAME, A.ID, A.COLID, A.TYPE$, A.LENGTH$, A.SCALE, A.NULLABLE$, A.DEFVAL, B.NAME AS TABLE_NAME, B.CRTDATE FROM SYSCOLUMNS AS a LEFT JOIN SYSOBJECTS AS B ON A.id = B.id WHERE B.name IN ('Text_len','T2','test_type') AND B.SCHID IN (SELECT ID FROM SYSOBJECTS WHERE name = 'SYSDBA');
/// NAME             |ID  |COLID|TYPE$                         |LENGTH$   |SCALE|NULLABLE$|DEFVAL            |TABLE_NAME|CRTDATE                |
/// -----------------+----+-----+------------------------------+----------+-----+---------+------------------+----------+-----------------------+
/// C1               |1155|    0|DATETIME WITH TIME ZONE       |        10|    2|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// C2               |1155|    1|TIMESTAMP                     |         8|    6|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// c3               |1155|    2|VARCHAR                       |       100|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// c4               |1155|    3|NUMERIC                       |         0|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// c5               |1155|    4|TIME WITH TIME ZONE           |         7|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// c6               |1155|    5|TIMESTAMP WITH LOCAL TIME ZONE|         8| 4102|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// NUMBER           |1155|    6|NUMBER                        |         0|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// DECIMAL          |1155|    7|DECIMAL                       |         0|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// BIT              |1155|    8|BIT                           |         1|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// INTEGER          |1155|    9|INTEGER                       |         4|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// xxx_PLS_INTEGER  |1155|   10|INTEGER                       |         4|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// BIGINT           |1155|   11|BIGINT                        |         8|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// TINYINT          |1155|   12|TINYINT                       |         1|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// BYTE             |1155|   13|BYTE                          |         1|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// SMALLINT         |1155|   14|SMALLINT                      |         2|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// BINARY           |1155|   15|BINARY                        |         1|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// VARBINARY        |1155|   16|VARBINARY                     |      8188|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// REAL             |1155|   17|REAL                          |         4|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// FLOAT            |1155|   18|FLOAT                         |         8|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// DOUBLE           |1155|   19|DOUBLE                        |         8|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// DOUBLE_PRECISION |1155|   20|DOUBLE PRECISION              |         8|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// CHAR             |1155|   21|CHAR                          |         1|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// VARCHAR          |1155|   22|VARCHAR                       |      8188|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// TEXT             |1155|   23|TEXT                          |2147483647|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// IMAGE            |1155|   24|IMAGE                         |2147483647|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// BLOB             |1155|   25|BLOB                          |2147483647|    0|Y        |                  |T2        |2022-09-21 09:06:07.633|
/// not_null_test    |1155|   26|VARCHAR                       |       100|    0|N        |'default_value_hh'|T2        |2022-09-21 09:06:07.633|
/// not_null_test_len|1155|   27|VARCHAR                       |       100|    0|N        |'default_value_hh'|T2        |2022-09-21 09:06:07.633|
/// id               |1145|    0|INT                           |         4|    0|Y        |                  |test_type |2022-09-16 09:53:55.521|
/// id_bigint        |1145|    1|BIGINT                        |         8|    0|Y        |                  |test_type |2022-09-16 09:53:55.521|
/// id_varchar       |1145|    2|VARCHAR                       |      8188|    0|Y        |                  |test_type |2022-09-16 09:53:55.521|
/// data_text        |1195|    0|TEXT                          |2147483647|    0|Y        |                  |Text_len  |2022-10-08 02:48:41.901|
/// ```
///
#[derive(Debug, Serialize, Deserialize)]
pub struct DmTableDesc {
    pub headers: BTreeMap<usize, ColNameEnum>,
    pub data: BTreeMap<String, Vec<DmTableItem>>,
}

impl Print for DmTableDesc {
    fn convert_table(self) -> anyhow::Result<Table> {
        let headers: Vec<StyledString> = self
            .headers
            .values()
            .map(|x| StyledString::new(x.to_string(), TextStyle::default_header()))
            .collect();

        let items: Vec<DmTableItem> = self.data.into_iter().fold(vec![], |mut vec, (_, mut x)| {
            vec.append(&mut x);
            vec
        });
        let rows = items
            .iter()
            .map(|x| {
                x.to_vec()
                    .into_iter()
                    .map(|x| StyledString::new(x, TextStyle::basic_left()))
                    .collect()
            })
            .collect::<Vec<Vec<StyledString>>>();

        Ok(Table::new(headers, rows, TableTheme::rounded()))
    }
}

impl DmTableDesc {
    pub fn new(headers: Vec<String>, data: Vec<Vec<String>>) -> anyhow::Result<Self> {
        macro_rules! to_type {
            ($val:expr,$t:ident) => {
                $val.parse::<$t>()
            };
        }

        let headers = headers
            .iter()
            .enumerate()
            .map(|(index, x)| (index, ColNameEnum::from_str(x).unwrap()))
            .fold(BTreeMap::default(), |mut m, (index, x)| {
                m.insert(index, x);
                m
            });

        let mut data_map: BTreeMap<String, Vec<DmTableItem>> = Default::default();

        for rows in data {
            assert_eq!(rows.len(), headers.len());
            // iterator row item
            let mut item = DmTableItem::default();
            for (index, val) in rows.into_iter().enumerate() {
                match headers.get(&index).unwrap() {
                    ColNameEnum::Name => item.name = val,
                    ColNameEnum::Id => item.table_id = to_type!(val, usize)?,
                    ColNameEnum::ColId => item.col_index = to_type!(val, usize)?,
                    ColNameEnum::Type => item.r#type = to_type!(val, DmDateType)?,
                    ColNameEnum::Length => item.length = to_type!(val, usize)?,
                    ColNameEnum::Scale => item.scale = to_type!(val, usize)?,
                    ColNameEnum::Nullable => match val.to_uppercase().as_ref() {
                        "Y" => item.nullable = true,
                        "N" => item.nullable = false,
                        _ => {}
                    },
                    ColNameEnum::DefaultVal => item.default_val = Some(val),
                    ColNameEnum::TableName => item.table_name = val,
                    ColNameEnum::CreateTime => item.create_time = val,
                    ColNameEnum::IsIdentity => match val.as_ref() {
                        "1" => item.is_identity = true,
                        "0" => item.is_identity = false,
                        _ => {}
                    },
                    ColNameEnum::SubType => item.subtype = Some(val),
                }
            }

            if let Some(items) = data_map.get_mut(&item.table_name) {
                items.push(item);
            } else {
                data_map.insert(item.table_name.to_owned(), vec![item]);
            }
        }
        Ok(DmTableDesc {
            headers,
            data: data_map,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::mock_table_result;

    #[test]
    fn test_dameng_table_desc_convert() {
        let result = mock_table_result();
        let dm_table_desc = DmTableDesc::new(result.0, result.1).unwrap();
        let string = format!("\n{}", dm_table_desc.table_string().unwrap());
        info!("{}", string);

        let expect = r#"
╭───────────────────┬──────┬───────┬────────────────────────────────┬────────────┬───────┬───────────┬────────────────────┬─────────────┬────────────────────────────┬─────────╮
│              NAME │  ID  │ COLID │             TYPE$              │  LENGTH$   │ SCALE │ NULLABLE$ │       DEFVAL       │ IS_IDENTITY │         TABLE_NAME         │ CRTDATE │
├───────────────────┼──────┼───────┼────────────────────────────────┼────────────┼───────┼───────────┼────────────────────┼─────────────┼────────────────────────────┼─────────┤
│ C1                │ 1058 │ 0     │ TIMESTAMP_WITH_TIME_ZONE       │ 10         │ 6     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ C2                │ 1058 │ 1     │ TIMESTAMP                      │ 8          │ 6     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ C3                │ 1058 │ 2     │ VARCHAR                        │ 100        │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ C4                │ 1058 │ 3     │ NUMERIC                        │ 0          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ C5                │ 1058 │ 4     │ TIME_WITH_TIME_ZONE            │ 7          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ C6                │ 1058 │ 5     │ TIMESTAMP_WITH_LOCAL_TIME_ZONE │ 8          │ 4102  │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ NUMBER            │ 1058 │ 6     │ NUMBER                         │ 0          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ DECIMAL           │ 1058 │ 7     │ DECIMAL                        │ 0          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ BIT               │ 1058 │ 8     │ BIT                            │ 1          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ INTEGER           │ 1058 │ 9     │ INTEGER                        │ 4          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ XXX_PLS_INTEGER   │ 1058 │ 10    │ INTEGER                        │ 4          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ BIGINT            │ 1058 │ 11    │ BIGINT                         │ 8          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ TINYINT           │ 1058 │ 12    │ TINYINT                        │ 1          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ BYTE              │ 1058 │ 13    │ BYTE                           │ 1          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ SMALLINT          │ 1058 │ 14    │ SMALLINT                       │ 2          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ BINARY            │ 1058 │ 15    │ BINARY                         │ 1          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ VARBINARY         │ 1058 │ 16    │ VARBINARY                      │ 8188       │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ REAL              │ 1058 │ 17    │ REAL                           │ 4          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ FLOAT             │ 1058 │ 18    │ FLOAT                          │ 8          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ DOUBLE            │ 1058 │ 19    │ DOUBLE                         │ 8          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ DOUBLE_PRECISION  │ 1058 │ 20    │ DOUBLE_PRECISION               │ 8          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ CHAR              │ 1058 │ 21    │ CHAR                           │ 1          │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ VARCHAR           │ 1058 │ 22    │ VARCHAR                        │ 8188       │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ TEXT              │ 1058 │ 23    │ TEXT                           │ 2147483647 │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ IMAGE             │ 1058 │ 24    │ IMAGE                          │ 2147483647 │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ BLOB              │ 1058 │ 25    │ BLOB                           │ 2147483647 │ 0     │ true      │                    │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ NOT_NULL_TEST     │ 1058 │ 26    │ VARCHAR                        │ 100        │ 0     │ false     │ 'default_value_hh' │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ NOT_NULL_TEST_LEN │ 1058 │ 27    │ VARCHAR                        │ 100        │ 0     │ false     │ 'default_value_hh' │ T2          │ 2022-10-24 17:28:26.308000 │         │
│ C1                │ 1058 │ 0     │ TIMESTAMP_WITH_TIME_ZONE       │ 10         │ 6     │ true      │                    │ T3          │ 2022-10-24 17:28:26.308000 │         │
│ CASE_SENSITIVE    │ 1058 │ 1     │ TIMESTAMP                      │ 8          │ 6     │ true      │                    │ T3          │ 2022-10-24 17:28:26.308000 │         │
│ C3                │ 1058 │ 2     │ VARCHAR                        │ 100        │ 0     │ true      │                    │ T3          │ 2022-10-24 17:28:26.308000 │         │
│ C4                │ 1058 │ 3     │ NUMERIC                        │ 0          │ 0     │ true      │                    │ T3          │ 2022-10-24 17:28:26.308000 │         │
│ NOT_NULL_TEST_LEN │ 1058 │ 4     │ VARCHAR                        │ 100        │ 0     │ false     │ 'default_value_hh' │ T3          │ 2022-10-24 17:28:26.308000 │         │
│ ID                │ 1058 │ 0     │ INTEGER                        │ 4          │ 0     │ false     │                    │ T4          │ 2022-10-24 17:28:26.308000 │         │
│ USER_ID           │ 1058 │ 1     │ VARCHAR                        │ 8188       │ 0     │ false     │                    │ T4          │ 2022-10-24 17:28:26.308000 │         │
│ USER_NAME         │ 1058 │ 2     │ TEXT                           │ 2147483647 │ 0     │ false     │                    │ T4          │ 2022-10-24 17:28:26.308000 │         │
│ ROLE              │ 1058 │ 3     │ TEXT                           │ 2147483647 │ 0     │ false     │                    │ T4          │ 2022-10-24 17:28:26.308000 │         │
│ SOURCE            │ 1058 │ 4     │ TEXT                           │ 2147483647 │ 0     │ false     │                    │ T4          │ 2022-10-24 17:28:26.308000 │         │
├───────────────────┼──────┼───────┼────────────────────────────────┼────────────┼───────┼───────────┼────────────────────┼─────────────┼────────────────────────────┼─────────┤
│              NAME │  ID  │ COLID │             TYPE$              │  LENGTH$   │ SCALE │ NULLABLE$ │       DEFVAL       │ IS_IDENTITY │         TABLE_NAME         │ CRTDATE │
╰───────────────────┴──────┴───────┴────────────────────────────────┴────────────┴───────┴───────────┴────────────────────┴─────────────┴────────────────────────────┴─────────╯"#;
        assert_eq!(string, expect);
    }
}
