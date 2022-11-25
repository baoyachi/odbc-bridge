#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SqlState {
    state_odbc: &'static str,
    state_pg: &'static str,
}

macro_rules! pg_sqlstate_mapping {
    (
        $(
            $(#[$docs:meta])*
            ($phrase:ident, $state_odbc:expr, $state_pg:expr);
        )+
    ) => {
        $(
            $(#[$docs])*
            #[allow(non_upper_case_globals)]
            pub const $phrase: SqlState = SqlState {
                state_odbc: $state_odbc,
                state_pg: $state_pg,
            };
        )+

        pub fn get_sqlstate_by_state_odbc(state_odbc: &str) -> Option<SqlState> {
            match state_odbc {
                $(
                    #[allow(unreachable_patterns)]
                    $state_odbc => Some($phrase),
                )+
                _ => None
            }
        }

        pub fn get_sqlstate_by_state_pg(state_pg: &str) -> Option<SqlState> {
            match state_pg {
                $(
                    #[allow(unreachable_patterns)]
                    $state_pg => Some($phrase),
                )+
                _ => None
            }
        }

        pub fn get_sqlstate_by_phrase(phrase: &str) -> Option<SqlState> {
            match phrase {
                $(
                    stringify!($phrase) => Some($phrase),
                )+
                _ => None
            }
        }

    }
}

pg_sqlstate_mapping! {
    ( STMT_ERROR_IN_ROW, "01S01", "01S01" );
    ( STMT_OPTION_VALUE_CHANGED, "01S02", "01S02" );
    ( STMT_ROW_VERSION_CHANGED,  "01001", "01001" ); /* data changed */
    ( STMT_POS_BEFORE_RECORDSET, "01S06", "01S06" );
    ( STMT_TRUNCATED, "01004", "01004" ); /* data truncated */
    ( STMT_INFO_ONLY, "00000", "00000" ); /* just an information that is returned, no error */

    ( STMT_OK,  "00000", "00000" ); /* OK */
    ( STMT_EXEC_ERROR, "HY000", "S1000" ); /* also a general error */
    ( STMT_STATUS_ERROR, "HY010", "S1010" );
    ( STMT_SEQUENCE_ERROR, "HY010", "S1010" ); /* Function sequence error */
    ( STMT_NO_MEMORY_ERROR, "HY001", "S1001" ); /* memory allocation failure */
    ( STMT_COLNUM_ERROR, "07009", "S1002" ); /* invalid column number */
    ( STMT_NO_STMTSTRING, "HY001", "S1001" ); /* having no stmtstring is also a malloc problem */
    ( STMT_ERROR_TAKEN_FROM_BACKEND, "HY000", "S1000" ); /* general error */
    ( STMT_INTERNAL_ERROR, "HY000", "S1000" ); /* general error */
    ( STMT_STILL_EXECUTING, "HY010", "S1010" );
    ( STMT_NOT_IMPLEMENTED_ERROR, "HYC00", "S1C00" ); /* == 'driver not
                              * capable' */
    ( STMT_BAD_PARAMETER_NUMBER_ERROR, "07009", "S1093" );
    ( STMT_OPTION_OUT_OF_RANGE_ERROR, "HY092", "S1092" );
    ( STMT_INVALID_COLUMN_NUMBER_ERROR, "07009", "S1002" );
    ( STMT_RESTRICTED_DATA_TYPE_ERROR, "07006", "07006" );
    ( STMT_INVALID_CURSOR_STATE_ERROR, "07005", "24000" );
    ( STMT_CREATE_TABLE_ERROR, "42S01", "S0001" ); /* table already exists */
    ( STMT_NO_CURSOR_NAME, "S1015", "S1015" );
    ( STMT_INVALID_CURSOR_NAME, "34000", "34000" );
    ( STMT_INVALID_ARGUMENT_NO, "HY024", "S1009" ); /* invalid argument value */
    ( STMT_ROW_OUT_OF_RANGE, "HY107", "S1107" );
    ( STMT_OPERATION_CANCELLED, "HY008", "S1008" );
    ( STMT_INVALID_CURSOR_POSITION, "HY109", "S1109" );
    ( STMT_VALUE_OUT_OF_RANGE, "HY019", "22003" );
    ( STMT_OPERATION_INVALID, "HY011", "S1011" );
    ( STMT_PROGRAM_TYPE_OUT_OF_RANGE, "?????", "?????" );
    ( STMT_BAD_ERROR, "08S01", "08S01" ); /* communication link failure */
    ( STMT_INVALID_OPTION_IDENTIFIER, "HY092", "HY092" );
    ( STMT_RETURN_NULL_WITHOUT_INDICATOR, "22002", "22002" );
    ( STMT_INVALID_DESCRIPTOR_IDENTIFIER, "HY091", "HY091" );
    ( STMT_OPTION_NOT_FOR_THE_DRIVER, "HYC00", "HYC00" );
    ( STMT_FETCH_OUT_OF_RANGE, "HY106", "S1106" );
    ( STMT_COUNT_FIELD_INCORRECT, "07002", "07002" );
    ( STMT_INVALID_NULL_ARG, "HY009", "S1009" );
    ( STMT_NO_RESPONSE, "08S01", "08S01" );
    ( STMT_COMMUNICATION_ERROR, "08S01", "08S01" );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_sqlstate_by_state_odbc() {
        let state_odbc = "08S01";
        let sql_state = get_sqlstate_by_state_odbc(state_odbc);
        assert!(sql_state.is_some());
        assert_eq!(sql_state.unwrap().state_pg, "08S01");
    }

    #[test]
    fn test_get_sqlstate_by_state_pg() {
        let state_pg = "S1009";
        let sql_state = get_sqlstate_by_state_pg(state_pg);
        assert!(sql_state.is_some());
        assert_eq!(sql_state.unwrap().state_odbc, "HY024");
    }

    #[test]
    fn test_get_sqlstate_by_phrase() {
        let phrase = "STMT_STATUS_ERROR";
        let sql_state = get_sqlstate_by_phrase(phrase);
        assert!(sql_state.is_some());
        assert_eq!(sql_state.unwrap().state_pg, "S1010");
    }
}
