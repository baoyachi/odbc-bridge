use odbc_api::{Connection, ParameterCollectionRef};

#[derive(Default)]
pub struct ExecResult {
    pub rows_affected: usize,
}

pub fn exec_result<S: Into<String>>(
    conn: Connection,
    sql: S,
    params: impl ParameterCollectionRef,
) -> anyhow::Result<ExecResult> {
    let mut stmt = conn.preallocate()?;
    stmt.execute(&sql.into(), params)?;
    let row_op = stmt.row_count()?;
    let result = row_op
        .map(|r| ExecResult { rows_affected: r })
        .unwrap_or(Default::default());
    Ok(result)
}
