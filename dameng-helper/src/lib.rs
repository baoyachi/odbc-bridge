#![deny(missing_debug_implementations)]

#[allow(non_camel_case_types)]
pub mod data_type;
pub mod error;

use std::collections::HashMap;
pub use data_type::*;
use odbc_api::buffers::TextRowSet;
use odbc_api::handles::StatementImpl;
use odbc_api::{Cursor, CursorImpl, ResultSetMetadata};
use std::str::FromStr;
use enumset::{EnumSet, EnumSetType};

#[derive(Debug, Default)]
pub struct DmColumnDesc {
    table_id: Option<usize>,
    // inner: Vec<DmColumnInner>,
}

#[derive(Debug)]
pub struct DmTable {
    // const name: &'static str = "name";
    // alias_column_name:
    // alias_column_names: HashMap<ColName, String>,
    // table:DmTable,
}

impl DmTable{

}

#[derive(EnumSetType, Debug,Hash)]
pub enum ColNameEnum {
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


impl ToString for ColNameEnum {
    fn to_string(&self) -> String {
        match self {
            ColNameEnum::Name => "NAME",
            ColNameEnum::Id => "ID",
            ColNameEnum::Colid => "COLID",
            ColNameEnum::Type => "TYPE$",
            ColNameEnum::Length => "LENGTH$",
            ColNameEnum::Scale => "SCALE",
            ColNameEnum::Nullable => "NULLABLE$",
            ColNameEnum::DefaultVal => "DEFVAL",
            ColNameEnum::TableName => "TABLE_NAME",
        }.to_string()
    }
}

impl ColNameEnum{
    fn column_name() -> HashMap<Self,String>{
        let set:EnumSet<ColNameEnum> = EnumSet::all();
        set.iter().fold(HashMap::new(), |mut map, col_name| {
            map.insert(col_name, col_name.to_string());
            map
        })
    }
}



#[derive(Debug)]
pub struct DmColumnX {
    name: String,
    id: usize,
    colid: usize,
    r#type: usize,
    length: usize,
    scale: usize,
    nullable: bool,
    default_val: String,
    table_name: String,
}

pub trait DmAdapter {
    fn get_table_sql(table_name: &str) -> String;
    fn get_table_desc(self) -> anyhow::Result<DmColumnDesc>;
}

impl DmAdapter for CursorImpl<StatementImpl<'_>> {
    fn get_table_sql(table_name: &str) -> String {
        // Use sql: `SELECT A.*, B.NAME AS TABLE_NAME FROM SYSCOLUMNS AS a LEFT JOIN SYSOBJECTS AS B ON A.id = B.id WHERE B.name IN ("X")`;
        // The X is table name;
        format!(
            r#"SELECT A.*, B.NAME AS TABLE_NAME FROM SYSCOLUMNS AS a LEFT JOIN SYSOBJECTS AS B ON A.id = B.id WHERE B.name IN ({});"#,
            table_name
        )
    }

    fn get_table_desc(mut self) -> anyhow::Result<DmColumnDesc> {
        let headers = self.column_names()?.collect::<Result<Vec<String>, _>>()?;

        //TODO remove
        assert_eq!(headers, vec!["name", "data_type", "table_id"]);

        let mut buffers = TextRowSet::for_cursor(1024, &mut self, Some(4096))?;
        let mut row_set_cursor = self.bind_buffer(&mut buffers)?;
        let mut table_desc = DmColumnDesc::default();

        while let Some(batch) = row_set_cursor.fetch()? {
            for row_index in 0..batch.num_rows() {
                let num_cols = batch.num_cols();

                //TODO below need change

                assert_eq!(num_cols, headers.len());


                let mut row_data: Vec<_> = (0..num_cols)
                    .map(|col_index| batch.at(col_index, row_index).unwrap_or(&[]))
                    .into_iter()
                    .map(String::from_utf8_lossy)
                    .collect();
                // table_desc.inner.push(DmColumnInner::new(
                //     row_data.remove(0).to_string(),
                //     DataType::from_str(row_data.remove(0).as_ref())?,
                // ));
                if table_desc.table_id.is_none() {
                    table_desc.table_id = Some(row_data.remove(0).parse::<usize>()?);
                }
            }
        }
        Ok(table_desc)
    }
}
