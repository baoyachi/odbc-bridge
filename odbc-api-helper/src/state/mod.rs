use std::fmt;

use odbc_common::error::OdbcStdError;
use pg_helper::state::PgState;

use crate::TryConvert;

odbc_common::sqlstate_mapping! {
    OdbcState,
    ( STMT_OK,   "00000" ); /* OK */
    ( STMT_WARN,   "01000" ); /* warning */
    ( STMT_TRUNCATED,  "01004" ); /* String data, right truncated */
    ( STMT_INTERNAL_ERROR,  "HY000" ); /* general error */
    ( STMT_SEQUENCE_ERROR, "HY010" ); /* Function sequence error */
    ( STMT_NO_MEMORY_ERROR,  "HY001" ); /* memory allocation failure */
    ( STMT_INVALID_INDEX_ERROR,  "07009" ); /* invalid index */
    ( STMT_NOT_IMPLEMENTED_ERROR,  "HYC00" ); /* == 'driver not
                              * capable' */
    ( STMT_INVALID_IDENTIFER_ERROR,  "HY091" );
    ( STMT_RESTRICTED_DATA_TYPE_ERROR,  "07006" );
    ( STMT_CREATE_TABLE_ERROR,  "42S01" ); /* table already exists */
    ( STMT_INVALID_CURSOR_NAME,  "34000" );
    ( STMT_INVALID_CURSOR_STATE_ERROR, "24000");
    ( STMT_INVALID_ARGUMENT_NO,  "HY024" ); /* invalid argument value */
    ( STMT_ROW_OUT_OF_RANGE,  "HY107" );
    ( STMT_VALUE_OUT_OF_RANGE,  "HY019" );
    ( STMT_PROGRAM_TYPE_OUT_OF_RANGE,  "?????" );
    ( STMT_RETURN_NULL_WITHOUT_INDICATOR,  "22002" );
    ( STMT_FETCH_OUT_OF_RANGE,  "HY106" ); /* Fetch type out of range */
    ( STMT_INVALID_NULL_ARG,  "HY009" );
    ( STMT_COMMUNICATION_ERROR, "08S01" );
}

crate::sqlstate_try_convert! {
    PgState,
    OdbcState,
    ( STMT_OK); /* OK */
    ( STMT_WARN); /* warning */
    ( STMT_TRUNCATED); /* String data, right truncated */
    ( STMT_INTERNAL_ERROR ); /* general error */
    ( STMT_SEQUENCE_ERROR); /* Function sequence error */
    ( STMT_NO_MEMORY_ERROR); /* memory allocation failure */
    ( STMT_INVALID_INDEX_ERROR); /* invalid index */
    ( STMT_NOT_IMPLEMENTED_ERROR ); /* == 'driver not
                              * capable' */
    ( STMT_INVALID_IDENTIFER_ERROR);
    ( STMT_RESTRICTED_DATA_TYPE_ERROR );
    ( STMT_CREATE_TABLE_ERROR ); /* table already exists */
    ( STMT_INVALID_CURSOR_NAME);
    ( STMT_INVALID_CURSOR_STATE_ERROR);
    ( STMT_INVALID_ARGUMENT_NO ); /* invalid argument value */
    ( STMT_ROW_OUT_OF_RANGE );
    ( STMT_VALUE_OUT_OF_RANGE);
    ( STMT_PROGRAM_TYPE_OUT_OF_RANGE );
    ( STMT_RETURN_NULL_WITHOUT_INDICATOR );
    ( STMT_FETCH_OUT_OF_RANGE ); /* Fetch type out of range */
    ( STMT_INVALID_NULL_ARG);
    ( STMT_COMMUNICATION_ERROR );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
    struct TestStruct {
        pg_state: OdbcState,
        pg_msg: String,
    }
    #[test]
    fn test_odbc_state() {
        let test_struct = TestStruct {
            pg_state: OdbcState::STMT_WARN,
            pg_msg: "test".to_string(),
        };
        let test_struct_string = serde_json::to_value(test_struct).unwrap().to_string();
        let test_struct: TestStruct = serde_json::from_str(&test_struct_string).unwrap();
        assert_eq!(test_struct.pg_state, OdbcState::STMT_WARN);
        let odbc_ss = test_struct.pg_state.to_string();
        assert_eq!(odbc_ss, "01000");

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
