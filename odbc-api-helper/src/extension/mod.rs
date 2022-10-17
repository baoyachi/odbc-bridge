use std::fmt::Debug;

pub mod dameng;
pub mod odbc;
pub mod pg;

pub trait ColumnInto: Debug {}
