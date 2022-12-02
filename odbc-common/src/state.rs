sqlstate_mapping! {
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
