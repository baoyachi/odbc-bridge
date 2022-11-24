use odbc_api::Error;

#[derive(Debug, Default)]
pub struct ExecResult {
    pub rows_affected: usize,
    pub error_info: Option<Error>,
}
