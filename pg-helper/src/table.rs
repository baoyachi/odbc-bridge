use crate::PgType;
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone)]
pub struct PgTableDesc {
    pub data: BTreeMap<String, Vec<PgTableItem>>,
}

impl PgTableDesc {
    pub fn get_data(&self, key: String, case_sensitive: bool) -> Option<&Vec<PgTableItem>> {
        let key = if case_sensitive {
            key.to_uppercase()
        } else {
            key
        };
        self.data.get(&key)
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
    pub default_val: Option<String>,
    pub table_name: String,
    pub create_time: String,
}

impl Default for PgTableItem {
    fn default() -> Self {
        PgTableItem {
            name: "".to_string(),
            table_id: 0,
            col_index: 0,
            r#type: PgType::UNKNOWN,
            length: 0,
            scale: 0,
            nullable: false,
            default_val: None,
            table_name: "".to_string(),
            create_time: "".to_string(),
        }
    }
}
