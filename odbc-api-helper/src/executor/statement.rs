use crate::error::OdbcHelperError;
use crate::executor::batch::{OdbcOperation, Operation};
use crate::odbc_api::parameter::InputParameter;
use crate::TryConvert;
use either::Either;
use std::any::Any;
use std::fmt::Debug;

pub(crate) type EitherInputParameter = Either<Vec<Box<dyn InputParameter>>, Box<dyn Any>>;

pub trait StatementInput {
    type Item: SqlValue;
    type Operation: Operation;

    fn to_value(self) -> Either<Vec<Self::Item>, Box<dyn Any>>;
    fn to_sql(&self) -> &str;

    fn operation(&self) -> Self::Operation {
        todo!()
    }

    fn input_values(self) -> Result<EitherInputParameter, OdbcHelperError>
    where
        Self: Sized,
    {
        let params: EitherInputParameter = self.try_convert()?;
        Ok(params)
    }
}

pub trait SqlValue {
    fn to_value(self) -> Either<Box<dyn InputParameter>, Box<dyn Any>>;
}

#[derive(Debug)]
pub struct Statement<T, O>
where
    T: Debug,
{
    /// The SQL query
    pub sql: String,
    /// The values for the SQL statement's parameters
    pub values: Vec<T>,
    /// odbc-bridge operation,most are ignored, unless the batch operation is used
    pub odbc_operation: Option<O>,
}

impl<T, O> Statement<T, O>
where
    T: SqlValue + Debug,
    O: Operation,
{
    pub fn new<S: Into<String>>(sql: S, values: Vec<T>) -> Self {
        Statement {
            sql: sql.into(),
            values,
            odbc_operation: None,
        }
    }

    pub fn operation(mut self, opt: O) -> Self {
        self.odbc_operation = Some(opt);
        self
    }
}

impl SqlValue for &str {
    fn to_value(self) -> Either<Box<dyn InputParameter>, Box<dyn Any>> {
        Either::Right(Box::new(()))
    }
}

impl SqlValue for String {
    fn to_value(self) -> Either<Box<dyn InputParameter>, Box<dyn Any>> {
        Either::Right(Box::new(()))
    }
}

impl SqlValue for () {
    fn to_value(self) -> Either<Box<dyn InputParameter>, Box<dyn Any>> {
        Either::Right(Box::new(()))
    }
}

impl<T, OP> StatementInput for Statement<T, OP>
where
    T: SqlValue + Debug,
    OP: Operation,
{
    type Item = T;
    type Operation = OdbcOperation;

    fn to_value(self) -> Either<Vec<T>, Box<dyn Any>> {
        Either::Left(self.values)
    }

    fn to_sql(&self) -> &str {
        &self.sql
    }
}

impl StatementInput for &str {
    type Item = Self;
    type Operation = OdbcOperation;

    fn to_value(self) -> Either<Vec<Self::Item>, Box<dyn Any>> {
        Either::Right(Box::new(()))
    }

    fn to_sql(&self) -> &str {
        self
    }
}

impl StatementInput for String {
    type Item = Self;
    type Operation = OdbcOperation;

    fn to_value(self) -> Either<Vec<Self::Item>, Box<dyn Any>> {
        Either::Right(Box::new(()))
    }

    fn to_sql(&self) -> &str {
        self
    }
}

/// TryConvert State `StatementInput` trait to `EitherBoxParams`
/// # Example
///
/// ```rust
/// use std::any::Any;
/// use either::Either;
/// use odbc_common::odbc_api::parameter::InputParameter;
/// use odbc_api_helper::executor::batch::OdbcOperation;
/// use odbc_api_helper::executor::statement::Statement;
/// use odbc_api_helper::extension::pg::PgValueInput;
/// use odbc_api_helper::TryConvert;
///
/// let statement:Statement<PgValueInput,OdbcOperation> = Statement::new("select * from empty where name=? and age=?",vec![
///     PgValueInput::Varchar("foo".into()),
///     PgValueInput::Int2(Some(8))
/// ]);
///
/// let left:Vec<Box<dyn InputParameter>> = statement.try_convert().unwrap().left().unwrap();
/// assert_eq!(left.len(),2);
///
/// let statement = "select * from empty where name=? and age=?";
///
/// let right_any:Box<dyn Any> = statement.try_convert().unwrap().right().unwrap();///
/// let right = right_any.downcast::<()>().unwrap();
/// assert_eq!(right,Box::new(()));
///
/// ```
///
impl<T: StatementInput> TryConvert<EitherInputParameter> for T {
    type Error = OdbcHelperError;

    fn try_convert(self) -> Result<EitherInputParameter, Self::Error> {
        match self.to_value() {
            Either::Left(values) => {
                let params: Result<Vec<_>, Self::Error> = values
                    .into_iter()
                    .map(|v| v.to_value())
                    .map(|x| {
                        x.left().ok_or_else(|| {
                            OdbcHelperError::SqlParamsError("value not include empty tuple".into())
                        })
                    })
                    .collect();
                Ok(Either::Left(params?))
            }
            Either::Right(values) => Ok(Either::Right(values)),
        }
    }
}
