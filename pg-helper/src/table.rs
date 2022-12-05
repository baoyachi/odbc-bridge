use crate::PgType;
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone)]
pub struct PgTableDesc {
    pub data: BTreeMap<String, Vec<PgTableItem>>,
}

impl PgTableDesc {
    pub fn get_data(
        &self,
        key: impl AsRef<str>,
        case_sensitive: bool,
    ) -> Option<&Vec<PgTableItem>> {
        if case_sensitive {
            self.data.get(key.as_ref())
        } else {
            self.data.get(&key.as_ref().to_uppercase())
        }
    }
}

#[derive(Debug, Clone)]
pub struct PgTableItem {
    // column name
    pub name: String,
    // pg table id
    pub table_id: usize,
    // column index
    pub col_index: usize,
    // pg data type
    pub r#type: PgType,
    // column date type length
    pub length: usize,
    // column date type scale
    pub scale: usize,
    pub nullable: bool,
    pub is_identity: bool,
    pub default_val: Option<String>,
    pub table_name: String,
    pub create_time: String,
    pub subtype: Option<String>,
}
