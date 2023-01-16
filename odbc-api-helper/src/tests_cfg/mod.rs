use crate::executor::database::{OdbcDbConnection, Options};
use crate::executor::SupportDatabase;
use odbc_common::odbc_api::Environment;
use once_cell::sync::Lazy;

const ODBC_DATABASE_URL: &str = "ODBC_DATABASE_URL";
const DAMENG_CONNECTION: &str = "Driver={DM8};Server=0.0.0.0;UID=SYSDBA;PWD=SYSDBA001;";

pub fn get_dameng_conn() -> OdbcDbConnection<'static> {
    std::env::set_var(ODBC_DATABASE_URL, DAMENG_CONNECTION);
    get_conn()
}

pub fn get_conn() -> OdbcDbConnection<'static> {
    let odbc_database_url = std::env::var(ODBC_DATABASE_URL).unwrap();
    pub static ENV: Lazy<Environment> = Lazy::new(|| Environment::new().unwrap());

    let conn = ENV
        .connect_with_connection_string(&odbc_database_url)
        .unwrap();

    let connection = OdbcDbConnection::new(conn, Options::new(SupportDatabase::Dameng)).unwrap();
    connection
}
