use crate::DataType;
use std::collections::BTreeMap;
use std::str::FromStr;
use strum::{Display, EnumString};

pub type DmDateType = DataType;

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
#[derive(Debug)]
pub struct DmTableDesc {
    pub headers: BTreeMap<usize, ColNameEnum>,
    pub data: BTreeMap<String, Vec<DmTableItem>>,
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

        for rols in data {
            assert_eq!(rols.len(), headers.len());
            // iterator line item
            let mut item = DmTableItem::default();
            for (index, val) in rols.into_iter().enumerate() {
                match headers.get(&index).unwrap() {
                    ColNameEnum::Name => item.name = val,
                    ColNameEnum::Id => item.table_id = to_type!(val, usize)?,
                    ColNameEnum::Colid => item.col_index = to_type!(val, usize)?,
                    ColNameEnum::Type => item.r#type = to_type!(val, DataType)?,
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

#[derive(Debug, Default)]
pub struct DmTableItem {
    pub name: String,
    pub table_id: usize,
    pub col_index: usize,
    pub r#type: DmDateType,
    pub length: usize,
    pub scale: usize,
    pub nullable: bool,
    pub default_val: Option<String>,
    pub table_name: String,
    pub create_time: String,
}

#[derive(Debug, EnumString, Display)]
pub enum ColNameEnum {
    #[strum(to_string = "NAME")]
    Name,
    #[strum(to_string = "ID")]
    Id,
    #[strum(to_string = "COLID")]
    Colid,
    #[strum(to_string = "TYPE$")]
    Type,
    #[strum(to_string = "LENGTH$")]
    Length,
    #[strum(to_string = "SCALE")]
    Scale,
    #[strum(to_string = "NULLABLE$")]
    Nullable,
    #[strum(to_string = "DEFVAL")]
    DefaultVal,
    #[strum(to_string = "TABLE_NAME")]
    TableName,
    #[strum(to_string = "CRTDATE")]
    CreateTime,
}
