use odbc_common::{Print, StyledString, Table, TableTheme, TextStyle};

pub type TableDescResult = (Vec<String>, Vec<Vec<String>>);

#[derive(Debug)]
pub struct TableDescResultInner {
    pub column_names: Vec<String>,
    pub columns_desc: Vec<Vec<String>>,
}

impl From<TableDescResult> for TableDescResultInner {
    fn from(t: TableDescResult) -> Self {
        Self {
            column_names: t.0,
            columns_desc: t.1,
        }
    }
}

impl Print for TableDescResultInner {
    fn convert_table(self) -> anyhow::Result<Table> {
        let headers: Vec<StyledString> = self
            .column_names
            .iter()
            .map(|x| StyledString::new(x.to_string(), TextStyle::default_header()))
            .collect();

        let rows = self
            .columns_desc
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
