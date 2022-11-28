use crate::executor::batch::OdbcOperation;
use crate::executor::statement::StatementInput;
use either::Either;
use odbc_common::{Print, StyledString, Table, TableTheme, TextStyle};
use std::any::Any;

pub type TableDescResult = (Vec<String>, Vec<Vec<String>>);

pub type TableDescArgs<S1, S2> = (S1, Vec<S2>);
pub type TableDescArgsString = (String, Vec<String>);

struct TableDescArgsWrap<S1, S2> {
    inner: TableDescArgs<S1, S2>,
}

impl<S1, S2> TableDescArgsWrap<S1, S2>
where
    S1: Into<String> + 'static,
    S2: Into<String> + 'static,
{
    fn map(self) -> TableDescArgs<String, String> {
        let args = self.inner;
        (
            args.0.into(),
            args.1.into_iter().map(|x| x.into()).collect(),
        )
    }
}

/// `TableDescArgs` impl `StatementInput` trait
/// # Example
///
/// ```rust
/// use odbc_api_helper::executor::statement::StatementInput;
/// use odbc_api_helper::executor::table::TableDescArgsString;
///
/// let args: TableDescArgsString = ("h1".to_string(), vec!["h2".to_string()]);
/// let x = args.to_value().right().unwrap();
/// let args:Box<_> = x.downcast::<TableDescArgsString>().unwrap();
///
/// assert_eq!(args,Box::new(("h1".to_string(),vec!["h2".to_string()])));
/// ```
impl<S1, S2> StatementInput for TableDescArgs<S1, S2>
where
    S1: Into<String> + 'static,
    S2: Into<String> + 'static,
{
    type Item = ();
    type Operation = OdbcOperation;

    fn to_value(self) -> Either<Vec<Self::Item>, Box<dyn Any>> {
        let any = TableDescArgsWrap { inner: self }.map();
        Either::Right(Box::new(any))
    }

    fn to_sql(&self) -> &str {
        panic!("no need sql")
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
