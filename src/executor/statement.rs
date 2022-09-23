use crate::TryConvert;
use either::Either;
use odbc_api::parameter::InputParameter;

pub trait StatementInput {
    type Item: SqlValue;

    fn to_value(self) -> Either<Vec<Self::Item>, ()>;
    fn to_sql(&self) -> &str;
}

pub trait SqlValue {
    fn to_value(&self) -> Either<Box<dyn InputParameter>, ()>;
}

pub struct Statement<T> {
    /// The SQL query
    pub sql: String,
    /// The values for the SQL statement's parameters
    pub values: Vec<T>,
}

impl<T> Statement<T>
where
    T: SqlValue,
{
    pub fn new<S: Into<String>>(sql: S, values: Vec<T>) -> Self {
        Statement {
            sql: sql.into(),
            values,
        }
    }
}

impl SqlValue for &str {
    fn to_value(&self) -> Either<Box<dyn InputParameter>, ()> {
        Either::Right(())
    }
}

impl SqlValue for String {
    fn to_value(&self) -> Either<Box<dyn InputParameter>, ()> {
        Either::Right(())
    }
}

impl<T> StatementInput for Statement<T>
where
    T: SqlValue,
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

pub type EitherBoxParams = Either<Vec<Box<dyn InputParameter>>, ()>;

impl<T: StatementInput> TryConvert<EitherBoxParams> for T {
    type Error = &'static str;

    fn try_convert(self) -> Result<EitherBoxParams, Self::Error> {
        match self.to_value() {
            Either::Left(values) => {
                let params: Result<Vec<_>, Self::Error> = values
                    .into_iter()
                    .map(|v| v.to_value())
                    .map(|x| x.left().ok_or("value not include empty tuple"))
                    .collect();
                Ok(Either::Left(params?))
            }
            Either::Right(values) => Ok(Either::Right(values)),
        }
    }
}
