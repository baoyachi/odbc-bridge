use crate::executor::batch::BatchResult;
use crate::executor::batch::Operation;
use crate::executor::execute::ExecResult;
use crate::executor::query::QueryResult;
use crate::executor::statement::StatementInput;
use crate::executor::table::{TableDescArgsString, TableDescResult};
use crate::executor::SupportDatabase;
use crate::extension::odbc::{OdbcColumn, OdbcColumnItem};
use crate::{Convert, TryConvert};
use dameng_helper::DmAdapter;
use either::Either;
use odbc_common::error::OdbcStdError;
use odbc_common::error::OdbcStdResult;
use odbc_common::error::OdbcWrapperError;
use odbc_common::odbc_api::{
    buffers::{AnySlice, BufferDesc, ColumnarAnyBuffer},
    handles::StatementImpl,
    ColumnDescription, Connection, Cursor, CursorImpl, ParameterCollectionRef, ResultSetMetadata,
};
use std::ops::IndexMut;

pub trait ConnectionTrait {
    /// Execute a `[Statement]`  INSERT,UPDATE,DELETE
    fn execute<S>(&self, stmt: S) -> OdbcStdResult<ExecResult>
    where
        S: StatementInput;

    /// Execute a `[Statement]` and return a collection Vec<[QueryResult]> on success
    fn query<S>(&self, stmt: S) -> OdbcStdResult<QueryResult>
    where
        S: StatementInput;

    fn show_table<S>(&self, stmt: S) -> OdbcStdResult<TableDescResult>
    where
        S: StatementInput;

    fn batch<S>(&self, stmt: Vec<S>) -> OdbcStdResult<BatchResult>
    where
        S: StatementInput;

    // begin transaction
    fn begin(&self) -> OdbcStdResult<()>;

    // finish transaction
    fn finish(&self) -> OdbcStdResult<()>;

    fn commit(&self) -> OdbcStdResult<()>;

    fn rollback(&self) -> OdbcStdResult<()>;
}

#[allow(missing_debug_implementations)]
pub struct OdbcDbConnection<'a> {
    pub conn: Connection<'a>,
    pub options: Options,
}

#[derive(Debug)]
pub struct Options {
    pub database: SupportDatabase,
    pub max_batch_size: usize,
    pub max_str_len: usize,
    pub max_binary_len: usize,
    // ignore uppercase/lowercase,default is false.
    // false:all column name convert uppercase
    // true: ignore，keep original column name
    pub case_sensitive: bool,
}

impl Options {
    // Default Max Buffer Size 256
    pub const MAX_BATCH_SIZE: usize = 1 << 7;
    // Default Max string length 1K
    pub const MAX_STR_LEN: usize = 1024;
    // Default Max binary length 1MB
    pub const MAX_BINARY_LEN: usize = 1024 * 1024;

    pub fn new(database: SupportDatabase) -> Self {
        Options {
            database,
            max_batch_size: Self::MAX_BATCH_SIZE,
            max_str_len: Self::MAX_STR_LEN,
            max_binary_len: Self::MAX_BINARY_LEN,
            case_sensitive: false,
        }
    }

    fn check(mut self) -> Self {
        if self.max_batch_size == 0 {
            self.max_batch_size = Self::MAX_BATCH_SIZE
        }

        if self.max_str_len == 0 {
            // Add default size:1K
            self.max_str_len = Self::MAX_STR_LEN
        }

        if self.max_binary_len == 0 {
            // Add default size:1MB
            self.max_binary_len = Self::MAX_BINARY_LEN
        }
        self
    }
}

impl<'a> ConnectionTrait for OdbcDbConnection<'a> {
    fn execute<S>(&self, stmt: S) -> OdbcStdResult<ExecResult>
    where
        S: StatementInput,
    {
        let sql = stmt.to_sql().to_string();
        match stmt.input_values()? {
            Either::Left(params) => self.exec_result(sql, &params[..]),
            Either::Right(_) => self.exec_result(sql, ()),
        }
    }

    fn query<S>(&self, stmt: S) -> OdbcStdResult<QueryResult>
    where
        S: StatementInput,
    {
        let sql = stmt.to_sql().to_string();

        match stmt.input_values()? {
            Either::Left(params) => self.query_result(&sql, &params[..]),
            Either::Right(_) => self.query_result(&sql, ()),
        }
    }

    /// The `TableDescArgs` impl  `StatementInput` trait.
    fn show_table<S>(&self, stmt: S) -> OdbcStdResult<TableDescResult>
    where
        S: StatementInput,
    {
        let any = stmt.to_value().right().ok_or_else(|| {
            OdbcStdError::OdbcError(OdbcWrapperError::DataHandlerError(
                "expect table desc args".to_string(),
            ))
        })?;
        let args = any.downcast::<TableDescArgsString>().map_err(|_| {
            OdbcStdError::TypeConversionError("cast TableDescArgsString error".to_string())
        })?;
        self.table_desc(args.0, args.1)
    }

    fn batch<S>(&self, stmt: Vec<S>) -> OdbcStdResult<BatchResult>
    where
        S: StatementInput,
    {
        self.begin()?;
        let mut batch_result = BatchResult::default();
        // TODO 1. Consider the current execution in the transaction
        // TODO 2. need change to parallel execution
        // TODO 3. consider when execute try_for_each result return error, transaction need rollback
        // the detail link:<https://github.com/baoyachi/odbc-bridge/issues/38>
        let result = stmt.into_iter().try_for_each(|s| {
            let op = s.operation();
            op.call(self, s, &mut batch_result)
        });
        match result {
            Ok(_) => {
                self.commit()?;
                self.finish()?;
                Ok(batch_result)
            }
            Err(err) => {
                self.rollback()?;
                Err(err)
            }
        }
    }

    fn begin(&self) -> OdbcStdResult<()> {
        Ok(self.conn.set_autocommit(false)?)
    }

    fn finish(&self) -> OdbcStdResult<()> {
        self.conn.set_autocommit(true)?;
        Ok(())
    }

    fn commit(&self) -> OdbcStdResult<()> {
        self.conn.commit()?;
        Ok(())
    }

    fn rollback(&self) -> OdbcStdResult<()> {
        self.conn.rollback()?;
        Ok(())
    }
}

impl<'a> OdbcDbConnection<'a> {
    pub fn new(conn: Connection<'a>, options: Options) -> OdbcStdResult<Self> {
        let options = options.check();
        let connection = Self { conn, options };
        Ok(connection)
    }

    fn exec_result<S: Into<String>>(
        &self,
        sql: S,
        params: impl ParameterCollectionRef,
    ) -> OdbcStdResult<ExecResult> {
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
    ) -> OdbcStdResult<QueryResult> {
        let mut cursor = self.conn.execute(sql, params)?.ok_or_else(|| {
            OdbcStdError::OdbcError(OdbcWrapperError::DataHandlerError(
                "query error".to_string(),
            ))
        })?;

        let mut query_result = Self::get_cursor_columns(&mut cursor)?;
        debug!("columns:{:?}", query_result.columns);

        let descs = query_result.columns.iter().map(|c| {
            <(&OdbcColumn, &Options) as TryConvert<BufferDesc>>::try_convert((c, &self.options))
                .unwrap()
        });

        let row_set_buffer =
            ColumnarAnyBuffer::try_from_descs(self.options.max_batch_size, descs).unwrap();

        let mut row_set_cursor = cursor.bind_buffer(row_set_buffer).unwrap();

        let mut total_row = vec![];
        while let Some(row_set) = row_set_cursor.fetch()? {
            for index in 0..query_result.columns.len() {
                let column_view: AnySlice = row_set.column(index);
                let column_types: Vec<OdbcColumnItem> = column_view.convert();
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

    fn get_cursor_columns(cursor: &mut CursorImpl<StatementImpl>) -> OdbcStdResult<QueryResult> {
        let mut query_result = QueryResult::default();
        for index in 0..cursor.num_result_cols()?.try_into()? {
            let mut column_description = ColumnDescription::default();
            cursor.describe_col(index + 1, &mut column_description)?;

            let column = OdbcColumn::new(
                column_description.name_to_string()?,
                column_description.data_type,
                column_description.could_be_nullable(),
            );
            query_result.columns.push(column);
        }
        Ok(query_result)
    }

    fn table_desc(
        &self,
        db_name: String,
        table_names: Vec<String>,
    ) -> OdbcStdResult<TableDescResult> {
        let db = &self.options.database;
        match db {
            SupportDatabase::Dameng => {
                let describe =
                    CursorImpl::get_table_sql(table_names, db_name, self.options.case_sensitive);
                let cursor = self
                    .conn
                    .execute(&describe.describe_sql, ())?
                    .ok_or_else(|| {
                        OdbcStdError::OdbcError(OdbcWrapperError::DataHandlerError(
                            "query error".to_string(),
                        ))
                    })?;
                cursor.get_table_desc(describe)
            }
            _ => Err(OdbcStdError::StringError(format!(
                "current not support database:{:?}",
                db
            ))),
        }
    }
}
