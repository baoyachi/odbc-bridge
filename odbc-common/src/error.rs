use std::{
    char::DecodeUtf16Error,
    fmt::{self, Display, Formatter},
    num::{ParseFloatError, ParseIntError, TryFromIntError},
    str::{ParseBoolError, Utf8Error},
};

use chrono::ParseError;
use odbc_api::handles::slice_to_cow_utf8;
use thiserror::Error;

pub type OdbcStdResult<T, E = OdbcStdError> = core::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum OdbcStdError {
    #[error("odbc error:`{0}`")]
    OdbcError(OdbcWrapperError),
    #[error("invalid sql params `{0}` error")]
    SqlParamsError(String),
    #[error("Failed to convert byte to {0}")]
    TypeConversionError(String),
    #[error("处理异常：{0}")]
    StringError(String),
}

impl Default for OdbcStdError {
    fn default() -> Self {
        OdbcStdError::StringError(String::new())
    }
}

impl From<odbc_api::Error> for OdbcStdError {
    fn from(e: odbc_api::Error) -> Self {
        OdbcStdError::OdbcError(e.into())
    }
}

impl From<TryFromIntError> for OdbcStdError {
    fn from(e: TryFromIntError) -> Self {
        OdbcStdError::TypeConversionError(e.to_string())
    }
}

impl From<DecodeUtf16Error> for OdbcStdError {
    fn from(e: DecodeUtf16Error) -> Self {
        OdbcStdError::OdbcError(OdbcWrapperError::DataHandlerError(e.to_string()))
    }
}

impl From<ParseIntError> for OdbcStdError {
    fn from(e: ParseIntError) -> Self {
        OdbcStdError::TypeConversionError(e.to_string())
    }
}

impl From<ParseBoolError> for OdbcStdError {
    fn from(e: ParseBoolError) -> Self {
        OdbcStdError::TypeConversionError(e.to_string())
    }
}

impl From<ParseFloatError> for OdbcStdError {
    fn from(e: ParseFloatError) -> Self {
        OdbcStdError::TypeConversionError(e.to_string())
    }
}

impl From<ParseError> for OdbcStdError {
    fn from(e: ParseError) -> Self {
        OdbcStdError::TypeConversionError(e.to_string())
    }
}

#[derive(Debug, Error)]
pub enum OdbcWrapperError {
    #[error("data handler error:`{0}`")]
    DataHandlerError(String),
    #[error("statement error:`{0}`")]
    StatementError(StatementError),
}

#[derive(Debug, Error)]
pub struct StatementError {
    pub state: String,
    pub error_msg: String,
}

impl Display for StatementError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "state: {:?}, error_msg: {:?}",
            self.state, self.error_msg
        )
    }
}

impl From<Utf8Error> for OdbcWrapperError {
    fn from(error: Utf8Error) -> Self {
        OdbcWrapperError::DataHandlerError(error.to_string())
    }
}

impl From<odbc_api::Error> for OdbcWrapperError {
    fn from(error: odbc_api::Error) -> Self {
        match &error {
            odbc_api::Error::Diagnostics { record, .. }
            | odbc_api::Error::UnsupportedOdbcApiVersion(record)
            | odbc_api::Error::InvalidRowArraySize { record, .. }
            | odbc_api::Error::UnableToRepresentNull(record)
            | odbc_api::Error::OracleOdbcDriverDoesNotSupport64Bit(record) => {
                let msg_info = slice_to_cow_utf8(&record.message);
                let state = match std::str::from_utf8(&record.state.0) {
                    Ok(state_data) => state_data,
                    Err(e) => {
                        return OdbcWrapperError::DataHandlerError(e.to_string());
                    }
                };
                return OdbcWrapperError::StatementError(StatementError {
                    state: state.to_string(),
                    error_msg: msg_info.to_string(),
                });
            }
            _ => {}
        }
        OdbcWrapperError::DataHandlerError(error.to_string())
    }
}
