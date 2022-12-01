use crate::odbc_api::Error;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum DmError {
    #[error("Failed to handle odbc-api error:{0}")]
    OdbcError(Error),
    #[error("Failed to parse Dameng DateType with str:{0}")]
    DataTypeError(String),
    #[error("Failed to Dameng DateType to_string error:{0}")]
    ToStringError(String),
}
