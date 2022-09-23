use crate::executor::execute::ExecResult;
use crate::executor::query::QueryResult;
use crate::executor::statement::{SqlValue, StatementInput};
use crate::extension::odbc::Column;
use crate::Convert;
use either::Either;
use odbc_api::buffers::{AnyColumnView, BufferDescription, ColumnarAnyBuffer};
use odbc_api::{ColumnDescription, Connection, Cursor, ParameterCollectionRef, ResultSetMetadata};
use std::ops::IndexMut;

pub trait ConnectionTrait {
    /// Execute a [Statement]  INSETT,UPDATE,DELETE
    fn execute<S>(&self, stmt: S) -> anyhow::Result<ExecResult>
    where
        S: StatementInput;

    /// Execute a [Statement] and return a collection Vec<[QueryResult]> on success
    fn query<S>(&self, stmt: S) -> anyhow::Result<QueryResult>
    where
        S: StatementInput;

    fn show_table(&self, table_name: &str) -> anyhow::Result<QueryResult>;

    // begin transaction
    fn begin(&self) -> anyhow::Result<()>;

    // finish transaction
    fn finish(&self) -> anyhow::Result<()>;

    fn commit(&self) -> anyhow::Result<()>;

    fn rollback(&self) -> anyhow::Result<()>;
}

pub struct OdbcDbConnection<'a> {
    conn: Connection<'a>,
    max_batch_size: Option<usize>,
}

impl<'a> ConnectionTrait for OdbcDbConnection<'a> {
    fn execute<S>(&self, stmt: S) -> anyhow::Result<ExecResult>
    where
        S: StatementInput,
    {
        let sql = stmt.to_sql().to_string();
        match stmt.to_value() {
            Either::Left(values) => {
                let params: Vec<_> = values
                    .into_iter()
                    .map(|v| v.to_value())
                    .map(|x| {
                        match x {
                            Either::Left(v) => v,
                            Either::Right(()) => {
                                //TODO fix: throws Error
                                panic!("value not include empty tuple")
                            }
                        }
                    })
                    .collect();
                self.exec_result(sql, &params[..])
            }
            Either::Right(()) => self.exec_result(sql, ()),
        }
    }

    fn query<S>(&self, stmt: S) -> anyhow::Result<QueryResult>
    where
        S: StatementInput,
    {
        let sql = stmt.to_sql().to_string();

        match stmt.to_value() {
            Either::Left(values) => {
                let params: Vec<_> = values
                    .into_iter()
                    .map(|v| v.to_value())
                    .map(|x| {
                        match x {
                            Either::Left(v) => v,
                            Either::Right(()) => {
                                //TODO fix: throws Error
                                panic!("value not include empty tuple")
                            }
                        }
                    })
                    .collect();
                self.query_result(&sql, &params[..])
            }
            Either::Right(()) => self.query_result(&sql, ()),
        }
    }

    fn show_table(&self, table_name: &str) -> anyhow::Result<QueryResult> {
        self.desc_table(table_name)
    }

    fn begin(&self) -> anyhow::Result<()> {
        Ok(self.conn.set_autocommit(false)?)
    }

    fn finish(&self) -> anyhow::Result<()> {
        self.conn.set_autocommit(true)?;
        Ok(())
    }

    fn commit(&self) -> anyhow::Result<()> {
        self.conn.commit()?;
        Ok(())
    }

    fn rollback(&self) -> anyhow::Result<()> {
        self.conn.rollback()?;
        Ok(())
    }
}

impl<'a> OdbcDbConnection<'a> {
    // Max Buffer Size 256
    pub const MAX_BATCH_SIZE: usize = 1 << 8;

    pub fn new<S: Into<String>>(conn: Connection<'a>) -> anyhow::Result<Self> {
        let connection = Self {
            conn,
            max_batch_size: Some(Self::MAX_BATCH_SIZE),
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

    fn query_result(
        &self,
        sql: &str,
        params: impl ParameterCollectionRef,
    ) -> anyhow::Result<QueryResult> {
        let mut cursor = self
            .conn
            .execute(sql, params)?
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
                let column_types: Vec<_> = column_view.convert();
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

    fn desc_table(&self, sql: &str) -> anyhow::Result<QueryResult> {
        let mut cursor = self
            .conn
            .execute(sql, ())?
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
