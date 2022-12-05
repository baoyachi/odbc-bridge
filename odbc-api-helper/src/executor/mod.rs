pub mod batch;
pub mod database;
pub mod execute;
pub mod query;
pub mod statement;
pub mod table;

#[derive(Debug, Clone, Copy)]
pub enum SupportDatabase {
    Dameng,
    Pg,
    Mysql,
}
