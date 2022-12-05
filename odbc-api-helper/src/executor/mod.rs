pub mod batch;
pub mod database;
pub mod execute;
pub mod query;
pub mod statement;
pub mod table;
pub mod prepare;

#[derive(Debug, Clone, Copy)]
pub enum SupportDatabase {
    Dameng,
    Pg,
    Mysql,
}
