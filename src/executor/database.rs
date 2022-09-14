use crate::executor::execute::ExecResult;
use crate::executor::query::QueryResult;
use crate::extension::odbc::Column;
use crate::Convert;
use odbc_api::buffers::{AnyColumnView, BufferDescription, ColumnarAnyBuffer};
use odbc_api::{ColumnDescription, Connection, Cursor, ParameterCollectionRef, ResultSetMetadata};
use std::ops::IndexMut;

pub struct Statement<T> {
    pub table_name: Option<String>,
    /// The SQL query
    pub sql: String,
    /// The values for the SQL statement's parameters
    pub values: Vec<T>,
}

pub trait SqlValue {}

pub trait ConnectionTrait {
    /// Execute a [Statement]  INSETT,UPDATE,DELETE
    fn execute<T: SqlValue>(&self, stmt: Statement<T>) -> anyhow::Result<ExecResult>;

    /// Execute a [Statement] and return a collection Vec<[QueryResult]> on success
    fn query<T: SqlValue>(&self, stmt: Statement<T>) -> anyhow::Result<QueryResult>;

    fn show_table(&self, table_name: &str) -> anyhow::Result<QueryResult>;
}

pub struct OdbcDbConnection<'a> {
    conn: Connection<'a>,
    max_batch_size: Option<usize>,
    desc_table_tpl: String,
}

impl<'a> ConnectionTrait for OdbcDbConnection<'a> {
    fn execute<T: SqlValue>(&self, stmt: Statement<T>) -> anyhow::Result<ExecResult> {
        self.exec_result(stmt.sql, ())
    }

    fn query<T: SqlValue>(&self, stmt: Statement<T>) -> anyhow::Result<QueryResult> {
        self.query_result(stmt, ())
    }

    fn show_table(&self, table_name: &str) -> anyhow::Result<QueryResult> {
        self.desc_table(table_name)
    }
}

impl<'a> OdbcDbConnection<'a> {
    // Max Buffer Size 256
    pub const MAX_BATCH_SIZE: usize = 1 << 8;
    pub const DESC_TEMPLATE_TABLE: &'static str = "__{TEMPLATE_TABLE_NAME}__";

    pub fn new<S: Into<String>>(conn: Connection<'a>, desc_table_tpl: S) -> anyhow::Result<Self> {
        let desc_table_tpl = desc_table_tpl.into();
        if !desc_table_tpl.contains(Self::DESC_TEMPLATE_TABLE) {
            return Err(anyhow!(
                "not contain {},e.g:`select * from employee limit 0`",
                Self::DESC_TEMPLATE_TABLE
            ));
        }
        let connection = Self {
            conn,
            max_batch_size: Some(Self::MAX_BATCH_SIZE),
            desc_table_tpl,
        };
        Ok(connection)
    }

    pub fn max_batch_size(self, size: usize) -> Self {
        let size = if size == 0 {
            Self::MAX_BATCH_SIZE
        } else {
            size
        };
        Self {
            max_batch_size: Some(size),
            ..self
        }
    }

    pub fn desc_table_sql(&self, table_name: &str) -> String {
        self.desc_table_tpl
            .replace(Self::DESC_TEMPLATE_TABLE, table_name)
    }

    fn exec_result<S: Into<String>>(
        &self,
        sql: S,
        params: impl ParameterCollectionRef,
    ) -> anyhow::Result<ExecResult> {
        let mut stmt = self.conn.preallocate()?;
        stmt.execute(&sql.into(), params)?;
        let row_op = stmt.row_count()?;
        let result = row_op
            .map(|r| ExecResult { rows_affected: r })
            .unwrap_or_default();
        Ok(result)
    }

    fn query_result<T: SqlValue>(
        &self,
        stmt: Statement<T>,
        params: impl ParameterCollectionRef,
    ) -> anyhow::Result<QueryResult> {
        let mut cursor = self
            .conn
            .execute(&stmt.sql, params)?
            .ok_or_else(|| anyhow!("query error"))?;

        let mut query_result = if let Some(table_name) = &stmt.table_name {
            QueryResult {
                column_names: self.desc_table(table_name)?.column_names,
                ..Default::default()
            }
        } else {
            QueryResult::default()
        };

        for index in 0..cursor.num_result_cols()?.try_into()? {
            let mut column_description = ColumnDescription::default();
            cursor.describe_col(index + 1, &mut column_description)?;

            let column = Column::new(
                column_description.name_to_string()?,
                column_description.data_type,
                column_description.could_be_nullable(),
            );
            query_result.columns.push(column);
        }

        let descs = query_result
            .columns
            .iter()
            .map(|c| <&Column as TryInto<BufferDescription>>::try_into(c).unwrap());

        let row_set_buffer = ColumnarAnyBuffer::from_description(
            self.max_batch_size.unwrap_or(Self::MAX_BATCH_SIZE),
            descs,
        );

        let mut row_set_cursor = cursor.bind_buffer(row_set_buffer).unwrap();

        let mut total_row = vec![];
        while let Some(row_set) = row_set_cursor.fetch()? {
            for index in 0..query_result.columns.len() {
                let column_view: AnyColumnView = row_set.column(index);
                let column_types = column_view.convert();
                if index == 0 {
                    for c in column_types.into_iter() {
                        total_row.push(vec![c]);
                    }
                } else {
                    for (col_index, c) in column_types.into_iter().enumerate() {
                        let row = total_row.index_mut(col_index);
                        row.push(c)
                    }
                }
            }
        }
        query_result.data = total_row;
        Ok(query_result)
    }

    fn desc_table(&self, table_name: &str) -> anyhow::Result<QueryResult> {
        let mut cursor = self
            .conn
            .execute(&self.desc_table_sql(table_name), ())?
            .ok_or_else(|| anyhow!("query error"))?;

        let mut query_result = QueryResult::default();
        for index in 0..cursor.num_result_cols()?.try_into()? {
            let mut column_description = ColumnDescription::default();
            cursor.describe_col(index + 1, &mut column_description)?;

            let column = Column::new(
                column_description.name_to_string()?,
                column_description.data_type,
                column_description.could_be_nullable(),
            );
            query_result
                .column_names
                .insert(column.name.clone(), index as usize);
            query_result.columns.push(column);
        }
        Ok(query_result)
    }
}
