use odbc_api::{Connection, Environment};
use once_cell::sync::Lazy;

const DAMENG_CONNECTION: &str = "Driver={DM8};Server=0.0.0.0;UID=SYSDBA;PWD=SYSDBA001;";

pub fn get_dameng_conn() -> Connection<'static> {
    pub static ENV: Lazy<Environment> = Lazy::new(|| Environment::new().unwrap());

    ENV.connect_with_connection_string(DAMENG_CONNECTION)
        .unwrap()
}
