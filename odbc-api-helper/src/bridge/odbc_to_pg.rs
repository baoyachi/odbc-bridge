use crate::TryConvert;
use odbc_common::error::OdbcStdError;
pub use odbc_common::state::OdbcState;
pub use pg_helper::state::PgState;

impl TryConvert<OdbcState> for PgState {
    type Error = OdbcStdError;

    fn try_convert(self) -> Result<OdbcState, Self::Error> {
        match self {
            PgState::STMT_OK => Ok(OdbcState::STMT_OK),
            PgState::STMT_WARN => Ok(OdbcState::STMT_WARN),
            PgState::STMT_TRUNCATED => Ok(OdbcState::STMT_TRUNCATED),
            PgState::STMT_INTERNAL_ERROR => Ok(OdbcState::STMT_INTERNAL_ERROR),
            PgState::STMT_SEQUENCE_ERROR => Ok(OdbcState::STMT_SEQUENCE_ERROR),
            PgState::STMT_NO_MEMORY_ERROR => Ok(OdbcState::STMT_NO_MEMORY_ERROR),
            PgState::STMT_INVALID_INDEX_ERROR => Ok(OdbcState::STMT_INVALID_INDEX_ERROR),
            PgState::STMT_NOT_IMPLEMENTED_ERROR => Ok(OdbcState::STMT_NOT_IMPLEMENTED_ERROR),
            PgState::STMT_INVALID_IDENTIFER_ERROR => Ok(OdbcState::STMT_INVALID_IDENTIFER_ERROR),
            PgState::STMT_RESTRICTED_DATA_TYPE_ERROR => {
                Ok(OdbcState::STMT_RESTRICTED_DATA_TYPE_ERROR)
            }
            PgState::STMT_CREATE_TABLE_ERROR => Ok(OdbcState::STMT_CREATE_TABLE_ERROR),
            PgState::STMT_INVALID_CURSOR_NAME => Ok(OdbcState::STMT_INVALID_CURSOR_NAME),
            PgState::STMT_INVALID_CURSOR_STATE_ERROR => {
                Ok(OdbcState::STMT_INVALID_CURSOR_STATE_ERROR)
            }
            PgState::STMT_INVALID_ARGUMENT_NO => Ok(OdbcState::STMT_INVALID_ARGUMENT_NO),
            PgState::STMT_ROW_OUT_OF_RANGE => Ok(OdbcState::STMT_ROW_OUT_OF_RANGE),
            PgState::STMT_VALUE_OUT_OF_RANGE => Ok(OdbcState::STMT_VALUE_OUT_OF_RANGE),
            PgState::STMT_PROGRAM_TYPE_OUT_OF_RANGE => {
                Ok(OdbcState::STMT_PROGRAM_TYPE_OUT_OF_RANGE)
            }
            PgState::STMT_RETURN_NULL_WITHOUT_INDICATOR => {
                Ok(OdbcState::STMT_RETURN_NULL_WITHOUT_INDICATOR)
            }
            PgState::STMT_FETCH_OUT_OF_RANGE => Ok(OdbcState::STMT_FETCH_OUT_OF_RANGE),
            PgState::STMT_INVALID_NULL_ARG => Ok(OdbcState::STMT_INVALID_NULL_ARG),
            PgState::STMT_COMMUNICATION_ERROR => Ok(OdbcState::STMT_COMMUNICATION_ERROR),
        }
    }
}
impl TryConvert<PgState> for OdbcState {
    type Error = OdbcStdError;

    fn try_convert(self) -> Result<PgState, Self::Error> {
        match self {
            OdbcState::STMT_OK => Ok(PgState::STMT_OK),
            OdbcState::STMT_WARN => Ok(PgState::STMT_WARN),
            OdbcState::STMT_TRUNCATED => Ok(PgState::STMT_TRUNCATED),
            OdbcState::STMT_INTERNAL_ERROR => Ok(PgState::STMT_INTERNAL_ERROR),
            OdbcState::STMT_SEQUENCE_ERROR => Ok(PgState::STMT_SEQUENCE_ERROR),
            OdbcState::STMT_NO_MEMORY_ERROR => Ok(PgState::STMT_NO_MEMORY_ERROR),
            OdbcState::STMT_INVALID_INDEX_ERROR => Ok(PgState::STMT_INVALID_INDEX_ERROR),
            OdbcState::STMT_NOT_IMPLEMENTED_ERROR => Ok(PgState::STMT_NOT_IMPLEMENTED_ERROR),
            OdbcState::STMT_INVALID_IDENTIFER_ERROR => Ok(PgState::STMT_INVALID_IDENTIFER_ERROR),
            OdbcState::STMT_RESTRICTED_DATA_TYPE_ERROR => {
                Ok(PgState::STMT_RESTRICTED_DATA_TYPE_ERROR)
            }
            OdbcState::STMT_CREATE_TABLE_ERROR => Ok(PgState::STMT_CREATE_TABLE_ERROR),
            OdbcState::STMT_INVALID_CURSOR_NAME => Ok(PgState::STMT_INVALID_CURSOR_NAME),
            OdbcState::STMT_INVALID_CURSOR_STATE_ERROR => {
                Ok(PgState::STMT_INVALID_CURSOR_STATE_ERROR)
            }
            OdbcState::STMT_INVALID_ARGUMENT_NO => Ok(PgState::STMT_INVALID_ARGUMENT_NO),
            OdbcState::STMT_ROW_OUT_OF_RANGE => Ok(PgState::STMT_ROW_OUT_OF_RANGE),
            OdbcState::STMT_VALUE_OUT_OF_RANGE => Ok(PgState::STMT_VALUE_OUT_OF_RANGE),
            OdbcState::STMT_PROGRAM_TYPE_OUT_OF_RANGE => {
                Ok(PgState::STMT_PROGRAM_TYPE_OUT_OF_RANGE)
            }
            OdbcState::STMT_RETURN_NULL_WITHOUT_INDICATOR => {
                Ok(PgState::STMT_RETURN_NULL_WITHOUT_INDICATOR)
            }
            OdbcState::STMT_FETCH_OUT_OF_RANGE => Ok(PgState::STMT_FETCH_OUT_OF_RANGE),
            OdbcState::STMT_INVALID_NULL_ARG => Ok(PgState::STMT_INVALID_NULL_ARG),
            OdbcState::STMT_COMMUNICATION_ERROR => Ok(PgState::STMT_COMMUNICATION_ERROR),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use odbc_common::state::get_obj_by_state;
    use serde::*;

    #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
    struct Foo {
        pg_state: OdbcState,
        pg_msg: String,
    }
    #[test]
    fn test_odbc_state() {
        let f = Foo {
            pg_state: OdbcState::STMT_WARN,
            pg_msg: "test".to_string(),
        };
        let json = serde_json::to_string(&f).unwrap();
        let f: Foo = serde_json::from_str(&json).unwrap();
        assert_eq!(f.pg_state, OdbcState::STMT_WARN);
        assert_eq!(f.pg_state.to_string(), "01000");

        assert_eq!(get_obj_by_state("01000").unwrap(), OdbcState::STMT_WARN);
    }

    #[test]
    fn test_try_convert() {
        let odbc_state = OdbcState::STMT_WARN;
        let pg_state = odbc_state.try_convert().unwrap();
        assert_eq!(pg_state.to_string(), "01000");

        let pg_state = PgState::STMT_COMMUNICATION_ERROR;
        let odbc_state = pg_state.try_convert().unwrap();
        assert_eq!(odbc_state.to_string(), "08S01");
    }
}
