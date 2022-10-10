use crate::PgType;

#[derive(Debug)]
pub struct PgTableItem {
    // column name
    name: String,
    // pg table id
    table_id: usize,
    // column index
    col_index: usize,
    // pg data type
    r#type: PgType,
    // column date type length
    length: usize,
    // column date type scale
    scale: usize,
    nullable: bool,
    default_val: Option<String>,
    table_name: String,
    create_time: String,
}
