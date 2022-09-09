use crate::executor::Print;
use crate::extension::odbc::{Column, ColumnItem};
use nu_table::{StyledString, Table, TableTheme, TextStyle};
use std::collections::HashMap;

const BATCH_SIZE: usize = 5000;

#[derive(Debug, Default)]
pub struct QueryResult {
    pub(crate) column_names: HashMap<String, usize>,
    pub columns: Vec<Column>,
    pub data: Vec<Vec<ColumnItem>>,
}

impl Print for QueryResult {
    fn covert_table(&self) -> Table {
        let headers: Vec<StyledString> = self
            .columns
            .iter()
            .map(|x| StyledString::new(x.name.to_string(), TextStyle::default_header()))
            .collect();

        let rows = self
            .data
            .iter()
            .map(|x| {
                x.iter()
                    .map(|y| y.to_string())
                    .map(|y| StyledString::new(y, TextStyle::basic_left()))
                    .collect::<Vec<_>>()
            })
            .collect();
        Table::new(headers, rows, TableTheme::rounded())
    }
}
