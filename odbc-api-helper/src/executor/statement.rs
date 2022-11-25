use crate::TryConvert;
use either::Either;
use odbc_api::parameter::InputParameter;
use odbc_common::error::{OdbcStdError, OdbcStdResult};
use std::fmt::Debug;

pub(crate) type EitherBoxParams = Either<Vec<Box<dyn InputParameter>>, ()>;

pub trait StatementInput {
    type Item: SqlValue;

    fn to_value(self) -> Either<Vec<Self::Item>, ()>;
    fn to_sql(&self) -> &str;

    fn values(self) -> OdbcStdResult<EitherBoxParams, OdbcStdError>
    where
        Self: Sized,
    {
        let params: EitherBoxParams = self.try_convert()?;
        Ok(params)
    }
}

pub trait SqlValue {
    fn to_value(self) -> Either<Box<dyn InputParameter>, ()>;
}

#[derive(Debug)]
pub struct Statement<T: Debug> {
    /// The SQL query
    pub sql: String,
    /// The values for the SQL statement's parameters
    pub values: Vec<T>,
}

impl<T> Statement<T>
where
    T: SqlValue + Debug,
{
    pub fn new<S: Into<String>>(sql: S, values: Vec<T>) -> Self {
        Statement {
            sql: sql.into(),
            values,
        }
    }
}

impl SqlValue for &str {
    fn to_value(self) -> Either<Box<dyn InputParameter>, ()> {
        Either::Right(())
    }
}

impl SqlValue for String {
    fn to_value(self) -> Either<Box<dyn InputParameter>, ()> {
        Either::Right(())
    }
}

impl<T> StatementInput for Statement<T>
where
    T: SqlValue + Debug,
{
    type Item = T;

    fn to_value(self) -> Either<Vec<T>, ()> {
        Either::Left(self.values)
    }

    fn to_sql(&self) -> &str {
        &self.sql
    }
}

impl StatementInput for &str {
    type Item = Self;

    fn to_value(self) -> Either<Vec<Self::Item>, ()> {
        Either::Right(())
    }

    fn to_sql(&self) -> &str {
        self
    }
}

impl StatementInput for String {
    type Item = Self;

    fn to_value(self) -> Either<Vec<Self::Item>, ()> {
        Either::Right(())
    }

    fn to_sql(&self) -> &str {
        self
    }
}

/// TryConvert State `StatementInput` trait to `EitherBoxParams`
/// # Example
///
/// ```rust
/// use either::Either;
/// use odbc_api::parameter::InputParameter;
/// use odbc_api_helper::executor::statement::Statement;
/// use odbc_api_helper::extension::pg::PgValueInput;
/// use odbc_api_helper::TryConvert;
///
/// let statement = Statement::new("select * from empty where name=? and age=?",vec![
///     PgValueInput::Varchar("foo".into()),
///     PgValueInput::Int2(8)
/// ]);
///
/// let left:Vec<Box<dyn InputParameter>> = statement.try_convert().unwrap().left().unwrap();
/// assert_eq!(left.len(),2);
///
/// let statement = "select * from empty where name=? and age=?";
///
/// let right:() = statement.try_convert().unwrap().right().unwrap();///
/// assert_eq!(right,());
///
/// ```
///
impl<T: StatementInput> TryConvert<EitherBoxParams> for T {
    type Error = OdbcStdError;

    fn try_convert(self) -> OdbcStdResult<EitherBoxParams, Self::Error> {
        match self.to_value() {
            Either::Left(values) => {
                let params: OdbcStdResult<Vec<_>, Self::Error> = values
                    .into_iter()
                    .map(|v| v.to_value())
                    .map(|x| {
                        x.left().ok_or_else(|| {
                            OdbcStdError::SqlParamsError("value not include empty tuple".into())
                        })
                    })
                    .collect();
                Ok(Either::Left(params?))
            }
            Either::Right(values) => Ok(Either::Right(values)),
        }
    }
}
