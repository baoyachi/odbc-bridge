use crate::executor::database::ConnectionTrait;
use crate::executor::execute::ExecResult;
use crate::executor::query::QueryResult;
use crate::executor::statement::StatementInput;
use crate::executor::table::TableDescResult;

pub trait Operation {
    fn call<Conn, S>(
        &self,
        conn: Conn,
        stmt: S,
        batch_result: &mut BatchResult,
    ) -> anyhow::Result<()>
    where
        Conn: ConnectionTrait,
        S: StatementInput;
}

#[derive(Debug)]
pub enum OdbcOperation {
    Execute,
    Query,
    ShowTable,
}

impl Operation for OdbcOperation {
    fn call<Conn, S>(
        &self,
        conn: Conn,
        stmt: S,
        batch_result: &mut BatchResult,
    ) -> anyhow::Result<()>
    where
        Conn: ConnectionTrait,
        S: StatementInput,
    {
        #[allow(clippy::unit_arg)]
        match self {
            OdbcOperation::Execute => Ok(conn.execute(stmt)?.to_batch(batch_result)),
            OdbcOperation::Query => Ok(conn.query(stmt)?.to_batch(batch_result)),
            OdbcOperation::ShowTable => Ok(conn.show_table(stmt)?.to_batch(batch_result)),
        }
    }
}

pub trait AnyBatchResult {
    fn to_batch(self, batch: &mut BatchResult);
}

impl AnyBatchResult for ExecResult {
    fn to_batch(self, batch: &mut BatchResult) {
        batch.execute.push(self);
    }
}

impl AnyBatchResult for QueryResult {
    fn to_batch(self, batch: &mut BatchResult) {
        batch.query.push(self);
    }
}

impl AnyBatchResult for TableDescResult {
    fn to_batch(self, batch: &mut BatchResult) {
        batch.table_desc.push(self);
    }
}

#[derive(Default, Debug)]
pub struct BatchResult {
    execute: Vec<ExecResult>,
    query: Vec<QueryResult>,
    table_desc: Vec<TableDescResult>,
}
