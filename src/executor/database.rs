use std::any::Any;
use std::error::Error;
use crate::executor::execute::ExecResult;
use crate::executor::query::QueryResult;

pub struct Statement {
    /// The SQL query
    pub sql: String,
    /// The values for the SQL statement's parameters
    pub values: Option<Box<dyn Any>>,
}

#[async_trait::async_trait]
pub trait ConnectionTrait: Sync {
    /// Execute a [Statement]  INSETT,UPDATE,DELETE
    async fn execute(&self, stmt: Statement) -> Result<ExecResult, Box<dyn Error>>;

    /// Execute a [Statement] and return a query SELECT
    async fn query_one(&self, stmt: Statement) -> Result<Option<QueryResult>, Box<dyn Error>>;

    /// Execute a [Statement] and return a collection Vec<[QueryResult]> on success
    async fn query_all(&self, stmt: Statement) -> Result<Vec<QueryResult>, Box<dyn Error>>;
}
