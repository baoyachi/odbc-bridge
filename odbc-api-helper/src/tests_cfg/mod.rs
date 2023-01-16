use crate::executor::database::{OdbcDbConnection, Options};
use crate::executor::SupportDatabase;
use odbc_common::odbc_api::Environment;
use once_cell::sync::Lazy;

const DAMENG_CONNECTION: &str = "Driver={DM8};Server=0.0.0.0;UID=SYSDBA;PWD=SYSDBA001;";

pub fn get_dameng_conn() -> OdbcDbConnection<'static> {
    pub static ENV: Lazy<Environment> = Lazy::new(|| Environment::new().unwrap());

    let conn = ENV
        .connect_with_connection_string(DAMENG_CONNECTION)
        .unwrap();

    let connection = OdbcDbConnection::new(conn, Options::new(SupportDatabase::Dameng)).unwrap();
    connection
}
