use either::Either;
use odbc_api::parameter::InputParameter;
use odbc_api::Bit;
use odbc_api::IntoParameter;

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

pub enum ValueInput {
    INT2(i16),
    INT4(i32),
    INT8(i64),
    FLOAT4(f32),
    FLOAT8(f64),
    CHAR(String),
    VARCHAR(String),
    TEXT(String),
    Bool(bool),
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

impl SqlValue for ValueInput {
    fn to_value(&self) -> Either<Box<dyn InputParameter>, ()> {
        macro_rules! left_param {
            ($($arg:tt)*) => {{
                Either::Left(Box::new($($arg)*))
            }};
        }

        match self {
            Self::INT2(i) => left_param!(i.into_parameter()),
            Self::INT4(i) => left_param!(i.into_parameter()),
            Self::INT8(i) => left_param!(i.into_parameter()),
            Self::FLOAT4(i) => left_param!(i.into_parameter()),
            Self::FLOAT8(i) => left_param!(i.into_parameter()),
            Self::CHAR(i) => left_param!(i.to_string().into_parameter()),
            Self::VARCHAR(i) => left_param!(i.to_string().into_parameter()),
            Self::TEXT(i) => left_param!(i.to_string().into_parameter()),
            Self::Bool(i) => left_param!(Bit::from_bool(*i).into_parameter()),
        }
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
