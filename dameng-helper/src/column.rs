use enumset::{EnumSet, EnumSetType};
use std::collections::HashMap;


#[derive(Debug)]
pub struct Table {
    pub headers: Vec<String>,
    pub data: Vec<Vec<String>>,
}

#[derive(EnumSetType, Debug, Hash)]
pub enum ColNameDesc {
    Name,
    Id,
    Colid,
    Type,
    Length,
    Scale,
    Nullable,
    DefaultVal,
    TableName,
}


impl ToString for ColNameDesc {
    fn to_string(&self) -> String {
        match self {
            ColNameDesc::Name => "NAME",
            ColNameDesc::Id => "ID",
            ColNameDesc::Colid => "COLID",
            ColNameDesc::Type => "TYPE$",
            ColNameDesc::Length => "LENGTH$",
            ColNameDesc::Scale => "SCALE",
            ColNameDesc::Nullable => "NULLABLE$",
            ColNameDesc::DefaultVal => "DEFVAL",
            ColNameDesc::TableName => "TABLE_NAME",
        }.to_string()
    }
}

impl ColNameDesc {
    fn column_name() -> HashMap<String, Self> {
        let set: EnumSet<ColNameDesc> = EnumSet::all();
        set.iter().fold(HashMap::new(), |mut map, col_name| {
            map.insert(col_name.to_string(), col_name);
            map
        })
    }
}