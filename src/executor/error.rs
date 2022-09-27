#[derive(Debug)]
pub enum Error {
    OdbcError(odbc_api::Error),
    UnknownError(String),
}
