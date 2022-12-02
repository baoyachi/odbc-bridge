odbc_common::sqlstate_mapping! {
    PgState,
    ( STMT_OK,   "00000" ); /* OK */
    ( STMT_WARN,   "01000" ); /* warning */
    ( STMT_TRUNCATED,  "01004" ); /* String data, right truncated */
    ( STMT_INTERNAL_ERROR,  "HV000" ); /* general error */
    ( STMT_SEQUENCE_ERROR, "HV010" ); /* Function sequence error */
    ( STMT_NO_MEMORY_ERROR,  "HV001" ); /* memory allocation failure */
    ( STMT_INVALID_INDEX_ERROR,  "HV00C" ); /* invalid index */
    ( STMT_NOT_IMPLEMENTED_ERROR,  "0A000" ); /* == 'driver not
                              * capable' */
    ( STMT_INVALID_IDENTIFER_ERROR,  "HV091" );
    ( STMT_RESTRICTED_DATA_TYPE_ERROR,  "23001" );
    ( STMT_CREATE_TABLE_ERROR,  "42P07" ); /* table already exists */
    ( STMT_INVALID_CURSOR_NAME,  "34000" );
    ( STMT_INVALID_CURSOR_STATE_ERROR, "24000");
    ( STMT_INVALID_ARGUMENT_NO,  "HV024" ); /* invalid argument value */
    ( STMT_ROW_OUT_OF_RANGE,  "P0003" );
    ( STMT_VALUE_OUT_OF_RANGE,  "22003" );
    ( STMT_PROGRAM_TYPE_OUT_OF_RANGE,  "?????" );
    ( STMT_RETURN_NULL_WITHOUT_INDICATOR,  "22002" );
    ( STMT_FETCH_OUT_OF_RANGE,  "42804" ); /* datatype_mismatch */
    ( STMT_INVALID_NULL_ARG,  "HV009" );
    ( STMT_COMMUNICATION_ERROR, "08000" );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
    struct Foo {
        pg_state: PgState,
        pg_msg: String,
    }
    #[test]
    fn test_pg_state() {
        let f = Foo {
            pg_state: PgState::STMT_OK,
            pg_msg: "test".to_string(),
        };
        let json = serde_json::to_string(&f).unwrap();
        let f: Foo = serde_json::from_str(&json).unwrap();
        assert_eq!(f.pg_state, PgState::STMT_OK);
        assert_eq!(f.pg_state.to_string(), "00000");

        assert_eq!(
            get_obj_by_state("08000").unwrap(),
            PgState::STMT_COMMUNICATION_ERROR
        );
    }
}
