use crate::extension::odbc::{OdbcColumnDescription, OdbcColumnItem};
use odbc_common::error::OdbcStdResult;
use odbc_common::print_table::Print;
use odbc_common::{StyledString, Table, TableTheme, TextStyle};

pub type OdbcRow = Vec<OdbcColumnItem>;

#[derive(Debug, Default)]
pub struct QueryResult {
    // table columns header
    pub columns: Vec<OdbcColumnDescription>,
    // table columns data
    pub data: Vec<OdbcRow>,
}

impl Print for QueryResult {
    fn convert_table(self) -> OdbcStdResult<Table> {
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
        Ok(Table::new(headers, rows, TableTheme::rounded()))
    }
}
