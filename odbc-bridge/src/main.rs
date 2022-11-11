use clap::Parser;
use odbc_api_helper::executor::database::{OdbcDbConnection, Options};
use odbc_api_helper::executor::SupportDatabase;
use odbc_api_helper::odbc_api::Environment;
use odbc_api_helper::Print;
use serde::{Deserialize, Serialize};
use std::fs;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long)]
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EnvConfig {
    connection: String,
    database: String,
    sql: String,
}

fn main() {
    simple_log::quick!();

    let args = Args::parse();
    let json = fs::read_to_string(args.path).unwrap();
    let config: EnvConfig = serde_json::from_str(&json).unwrap();
    println!("config:{:?}", config);
    let env = Environment::new().unwrap();
    let conn = env
        .connect_with_connection_string(&config.connection)
        .unwrap();

    let connection = OdbcDbConnection::new(conn, Options::new(SupportDatabase::Dameng)).unwrap();
    let cursor_impl = connection.conn.execute(&config.sql, ()).unwrap().unwrap();
    cursor_impl.print_all_tables().unwrap()
}
