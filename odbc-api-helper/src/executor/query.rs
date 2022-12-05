use crate::extension::odbc::{OdbcColumnDesc, OdbcColumnItem};
use odbc_common::error::OdbcStdResult;
use odbc_common::print_table::Print;

pub type OdbcRow = Vec<OdbcColumnItem>;

#[derive(Debug, Default)]
pub struct QueryResult {
    // table columns describe
    pub columns: Vec<OdbcColumnDesc>,
    // table columns data
    pub data: Vec<OdbcRow>,
}

impl Print for QueryResult {
    fn header_data(self) -> OdbcStdResult<(Vec<String>, Vec<Vec<String>>)> {
        let headers: Vec<String> = self.columns.iter().map(|x| x.name.to_string()).collect();

        let data = self
            .data
            .iter()
            .map(|x| x.iter().map(|y| y.to_string()).collect::<Vec<_>>())
            .collect();
        Ok((headers, data))
    }
}
