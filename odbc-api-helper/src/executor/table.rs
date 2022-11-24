use either::Either;
use odbc_api::parameter::InputParameter;
use odbc_common::{Print, StyledString, Table, TableTheme, TextStyle};
use crate::executor::batch::OdbcOperation;
use crate::executor::statement::{SqlValue, StatementInput};

pub type TableDescResult = (Vec<String>, Vec<Vec<String>>);

pub type TableDescArgs<'a> = (&'a str, Vec<&'a str>);

impl<'a> StatementInput for TableDescArgs<'a>{
    type Item = Self;
    type Operation = OdbcOperation;

    fn to_value(self) -> Either<Vec<Self::Item>, ()> {
        Either::Left(vec![self])
    }

    fn to_sql(&self) -> &str {
        panic!("no need sql")
    }
}

impl<'a> SqlValue for TableDescArgs<'a>{

    fn to_value(self) -> Either<Box<dyn InputParameter>, ()> {
        todo!()
        // Either::Right(())
    }
}


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
