use crate::DataType;
use std::collections::BTreeMap;
use strum::{Display, EnumString};

/// The table data
/// ```bash
/// > run you code...
/// NAME             |ID  |COLID|TYPE$                         |LENGTH$   |SCALE|NULLABLE$|DEFVAL            |INFO1|INFO2|TABLE_NAME|
/// -----------------+----+-----+------------------------------+----------+-----+---------+------------------+-----+-----+----------+
/// id               |1145|    0|INT                           |         4|    0|Y        |                  |    0|    0|test_type |
/// id_bigint        |1145|    1|BIGINT                        |         8|    0|Y        |                  |    0|    0|test_type |
/// id_varchar       |1145|    2|VARCHAR                       |      8188|    0|Y        |                  |    0|    0|test_type |
/// C1               |1155|    0|DATETIME WITH TIME ZONE       |        10|    2|Y        |                  |    0|    0|T2        |
/// C2               |1155|    1|TIMESTAMP                     |         8|    6|Y        |                  |    0|    0|T2        |
/// c3               |1155|    2|VARCHAR                       |       100|    0|Y        |                  |    0|    0|T2        |
/// c4               |1155|    3|NUMERIC                       |         0|    0|Y        |                  |    0|    0|T2        |
/// c5               |1155|    4|TIME WITH TIME ZONE           |         7|    0|Y        |                  |    0|    0|T2        |
/// c6               |1155|    5|TIMESTAMP WITH LOCAL TIME ZONE|         8| 4102|Y        |                  |    0|    0|T2        |
/// NUMBER           |1155|    6|NUMBER                        |         0|    0|Y        |                  |    0|    0|T2        |
/// DECIMAL          |1155|    7|DECIMAL                       |         0|    0|Y        |                  |    0|    0|T2        |
/// BIT              |1155|    8|BIT                           |         1|    0|Y        |                  |    0|    0|T2        |
/// INTEGER          |1155|    9|INTEGER                       |         4|    0|Y        |                  |    0|    0|T2        |
/// xxx_PLS_INTEGER  |1155|   10|INTEGER                       |         4|    0|Y        |                  |    0|    0|T2        |
/// BIGINT           |1155|   11|BIGINT                        |         8|    0|Y        |                  |    0|    0|T2        |
/// TINYINT          |1155|   12|TINYINT                       |         1|    0|Y        |                  |    0|    0|T2        |
/// BYTE             |1155|   13|BYTE                          |         1|    0|Y        |                  |    0|    0|T2        |
/// SMALLINT         |1155|   14|SMALLINT                      |         2|    0|Y        |                  |    0|    0|T2        |
/// BINARY           |1155|   15|BINARY                        |         1|    0|Y        |                  |    0|    0|T2        |
/// VARBINARY        |1155|   16|VARBINARY                     |      8188|    0|Y        |                  |    0|    0|T2        |
/// REAL             |1155|   17|REAL                          |         4|    0|Y        |                  |    0|    0|T2        |
/// FLOAT            |1155|   18|FLOAT                         |         8|    0|Y        |                  |    0|    0|T2        |
/// DOUBLE           |1155|   19|DOUBLE                        |         8|    0|Y        |                  |    0|    0|T2        |
/// DOUBLE_PRECISION |1155|   20|DOUBLE PRECISION              |         8|    0|Y        |                  |    0|    0|T2        |
/// CHAR             |1155|   21|CHAR                          |         1|    0|Y        |                  |    0|    0|T2        |
/// VARCHAR          |1155|   22|VARCHAR                       |      8188|    0|Y        |                  |    0|    0|T2        |
/// TEXT             |1155|   23|TEXT                          |2147483647|    0|Y        |                  |    0|    0|T2        |
/// IMAGE            |1155|   24|IMAGE                         |2147483647|    0|Y        |                  |    0|    0|T2        |
/// BLOB             |1155|   25|BLOB                          |2147483647|    0|Y        |                  |    0|    0|T2        |
/// not_null_test    |1155|   26|VARCHAR                       |       100|    0|N        |'default_value_hh'|    0|    0|T2        |
/// not_null_test_len|1155|   27|VARCHAR                       |       100|    0|N        |'default_value_hh'|    0|    0|T2        |
/// data_text        |1195|    0|TEXT                          |2147483647|    0|Y        |                  |    0|    0|Text_len  |
/// ```
///
#[derive(Debug)]
pub struct DmTableDesc {
    pub headers: BTreeMap<usize, ColNameEnum>,
    pub data: BTreeMap<String, Vec<DmTableItem>>,
}

impl DmTableDesc {
    pub fn new(
        headers: BTreeMap<usize, ColNameEnum>,
        data: Vec<Vec<String>>,
    ) -> anyhow::Result<Self> {
        macro_rules! to_type {
            ($val:expr,$t:ident) => {
                $val.parse::<$t>()
            };
        }

        let mut data_map: BTreeMap<String, Vec<DmTableItem>> = Default::default();

        for rols in data {
            assert_eq!(rols.len(), headers.len());
            // iterator line item
            let mut item = DmTableItem::default();
            for (index, val) in rols.into_iter().enumerate() {
                match headers.get(&index).unwrap() {
                    ColNameEnum::Name => item.name = val,
                    ColNameEnum::Id => item.id = to_type!(val, usize)?,
                    ColNameEnum::Colid => item.colid = to_type!(val, usize)?,
                    ColNameEnum::Type => item.type_ = Some(to_type!(val, DataType)?),
                    ColNameEnum::Length => item.length_ = to_type!(val, usize)?,
                    ColNameEnum::Scale => item.scale = to_type!(val, usize)?,
                    ColNameEnum::Nullable => match val.to_uppercase().as_ref() {
                        "Y" => item.nullable = true,
                        "N" => item.nullable = false,
                        _ => {}
                    },
                    ColNameEnum::DefaultVal => item.default_val = Some(val),
                    ColNameEnum::INFO1 => {}
                    ColNameEnum::INFO2 => {}
                    ColNameEnum::TableName => item.table_name = val,
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
    name: String,
    id: usize,
    colid: usize,
    type_: Option<DataType>,
    length_: usize,
    scale: usize,
    nullable: bool,
    default_val: Option<String>,
    table_name: String,
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
    #[strum(to_string = "INFO1")]
    INFO1,
    #[strum(to_string = "INFO2")]
    INFO2,
    #[strum(to_string = "TABLE_NAME")]
    TableName,
}
