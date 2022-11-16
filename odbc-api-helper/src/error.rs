use thiserror::Error;

#[derive(Error, Debug)]
pub enum OdbcHelperError {
    #[error("odbc error:`{0}`")]
    OdbcError(odbc_api::Error),
    #[error("invalid sql params `{0}` error")]
    SqlParamsError(String),
    #[error("Failed to convert byte to {0}")]
    TypeConversionError(String),
}
