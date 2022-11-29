#![deny(missing_debug_implementations)]
#[macro_use]
extern crate log;

#[allow(non_camel_case_types)]
pub mod data_type;
pub mod table;

pub use data_type::*;
use odbc_common::{
    error::OdbcStdResult,
    odbc_api::{
        buffers::TextRowSet, handles::StatementImpl, Cursor, CursorImpl, ResultSetMetadata,
    },
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[cfg(test)]
#[ctor::ctor]
fn init_test() {
    simple_log::quick!();
}

pub trait DmAdapter {
    fn get_table_sql(
        table_names: Vec<String>,
        db_name: String,
        case_sensitive: bool,
    ) -> TableSqlDescribe;
    fn get_table_desc(
        self,
        describe: TableSqlDescribe,
    ) -> OdbcStdResult<(Vec<String>, Vec<Vec<String>>)>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSqlDescribe {
    pub db_name: String,
    pub describe_sql: String,
    pub column_name_index: usize,
    pub table_name_index: usize,
    // See detail dameng database case_sensitive rule: <https://github.com/baoyachi/odbc-bridge/discussions/25>
    pub case_sensitive: bool,
}

impl DmAdapter for CursorImpl<StatementImpl<'_>> {
    fn get_table_sql(
        table_names: Vec<String>,
        db_name: String,
        case_sensitive: bool,
    ) -> TableSqlDescribe {
        // Use sql: `SELECT A.*, B.NAME AS TABLE_NAME FROM SYSCOLUMNS AS a LEFT JOIN SYSOBJECTS AS B ON A.id = B.id WHERE B.name IN ("X")`;
        // The X is table name;
        let tables = table_names
            .iter()
            .map(|x| format!("'{}'", x))
            .collect::<Vec<_>>()
            .join(",");
        let describe_sql = format!(
            r#"SELECT A.NAME, A.ID, A.COLID, A.TYPE$, A.LENGTH$, A.SCALE, A.NULLABLE$, A.DEFVAL, A.INFO2 AS IS_IDENTITY, B.NAME AS TABLE_NAME, B.CRTDATE, B.SUBTYPE$ FROM SYSCOLUMNS AS a LEFT JOIN SYSOBJECTS AS B ON A.id = B.id WHERE B.name IN ({}) AND B.SCHID IN (SELECT ID FROM SYSOBJECTS WHERE name = '{}');"#,
            tables, db_name
        );
        TableSqlDescribe {
            db_name,
            describe_sql,
            column_name_index: 0,
            table_name_index: 8,
            case_sensitive,
        }
    }

    fn get_table_desc(
        mut self,
        describe: TableSqlDescribe,
    ) -> OdbcStdResult<(Vec<String>, Vec<Vec<String>>)> {
        debug!("describe:{:?}", describe);
        let case_sensitive_fn = |row_index: usize, name: Cow<str>| -> String {
            if !describe.case_sensitive
                && (row_index == describe.column_name_index
                    || row_index == describe.table_name_index)
            {
                return name.to_uppercase();
            }
            name.to_string()
        };

        let headers = self.column_names()?.collect::<Result<Vec<String>, _>>()?;

        let mut buffers = TextRowSet::for_cursor(1024, &mut self, Some(4096))?;
        let mut row_set_cursor = self.bind_buffer(&mut buffers)?;

        let mut data = vec![];
        while let Some(batch) = row_set_cursor.fetch()? {
            for row_index in 0..batch.num_rows() {
                let num_cols = batch.num_cols();
                let row_data: Vec<String> = (0..num_cols)
                    .map(|col_index| (col_index, batch.at(col_index, row_index).unwrap_or(&[])))
                    .into_iter()
                    .map(|(col_index, x)| (col_index, String::from_utf8_lossy(x)))
                    .map(|(col_index, x)| case_sensitive_fn(col_index, x))
                    .collect();
                data.push(row_data);
            }
        }
        Ok((headers, data))
    }
}

#[cfg(test)]
mod tests {
    const DAMENG_CONNECTION: &str = "Driver={DM8};Server=0.0.0.0;UID=SYSDBA;PWD=SYSDBA001;";

    use odbc_api_helper::executor::database::{ConnectionTrait, OdbcDbConnection, Options};
    use odbc_api_helper::executor::execute::ExecResult;
    use odbc_api_helper::executor::table::{TableDescArgs, TableDescResult};
    use odbc_api_helper::executor::SupportDatabase;
    use odbc_common::odbc_api::Environment;
    use odbc_common::Print;
    use once_cell::sync::Lazy;
    use regex::Regex;

    #[macro_export]
    macro_rules! svec {
            () => {{
                let v = Vec::<String>::new();
                v

            }};
            ($($elem:expr),+ $(,)?) => {{
                let v = vec![
                    $( String::from($elem), )*
                ];
                v
            }};
        }

    pub static ENV: Lazy<Environment> = Lazy::new(|| Environment::new().unwrap());

    /// Validate dameng database crtdate datetime format value
    ///
    /// # Example
    ///
    /// ```rust
    ///
    /// assert_eq!(true, validate_crtdate("2022-10-24 17:28:26.308000"));
    /// assert_eq!(false, validate_crtdate("2022-10-24 17:28:26 308000"));
    /// ```
    fn validate_crtdate(x: &str) -> bool {
        let regex =
            Regex::new(r#"^[0-9]{4}-[0-9]{2}-[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}[.]{1}[0-9]{6}"#)
                .unwrap();
        regex.is_match(x)
    }

    fn get_dameng_conn() -> OdbcDbConnection<'static> {
        let conn = ENV
            .connect_with_connection_string(DAMENG_CONNECTION)
            .unwrap();

        let connection =
            OdbcDbConnection::new(conn, Options::new(SupportDatabase::Dameng)).unwrap();
        connection
    }

    #[test]
    fn test_print_all_tables() {
        let connection = get_dameng_conn();
        let cursor = connection
            .conn
            .execute(r#"SELECT * from SYSCOLUMNS limit 10;"#, ())
            .unwrap()
            .unwrap();
        cursor.print_all_tables().unwrap();
    }

    #[test]
    fn test_dameng_table_desc() {
        let connection = get_dameng_conn();

        //1. create table
        let create_table_t2 = r#"
CREATE TABLE SYSDBA.T2 (
	C1 DATETIME WITH TIME ZONE,
	C2 TIMESTAMP,
	c3 VARCHAR(100),
	c4 NUMERIC,
	c5 TIME WITH TIME ZONE,
	c6 TIMESTAMP WITH LOCAL TIME ZONE,
	"NUMBER" NUMBER,
	"DECIMAL" DECIMAL,
	"BIT" BIT,
	"INTEGER" INTEGER,
	xxx_PLS_INTEGER INTEGER,
	"BIGINT" BIGINT,
	"TINYINT" TINYINT,
	"BYTE" BYTE,
	"SMALLINT" SMALLINT,
	"BINARY" BINARY(1),
	"VARBINARY" VARBINARY(8188),
	"REAL" REAL,
	"FLOAT" FLOAT,
	"DOUBLE" DOUBLE,
	DOUBLE_PRECISION DOUBLE PRECISION,
	"CHAR" CHAR(1),
	"VARCHAR" VARCHAR(8188),
	TEXT TEXT,
	IMAGE IMAGE,
	"BLOB" BLOB,
	not_null_test VARCHAR(100) DEFAULT 'default_value_hh' NOT NULL,
	not_null_test_len VARCHAR(100) DEFAULT 'default_value_hh' NOT NULL
);"#;
        let exec_result: ExecResult = connection.execute(create_table_t2).unwrap();
        assert_eq!(exec_result.rows_affected, 0);

        let create_table_t3 = r#"
CREATE TABLE SYSDBA.t3 (
	C1 DATETIME WITH TIME ZONE,
	case_seNSItive TIMESTAMP,
	c3 VARCHAR(100),
	c4 NUMERIC,
	not_null_test_len VARCHAR(100) DEFAULT 'default_value_hh' NOT NULL
);"#;
        let exec_result: ExecResult = connection.execute(create_table_t3).unwrap();
        assert_eq!(exec_result.rows_affected, 0);

        let create_table_t4 = r#"
CREATE TABLE SYSDBA.T4 (
	id INT NOT NULL,
	useR_ID CHARACTER VARYING(8188) NOT NULL,
	user_name TEXT NOT NULL,
	"role" TEXT NOT NULL,
	"source" TEXT NOT NULL
);"#;

        let exec_result: ExecResult = connection.execute(create_table_t4).unwrap();
        assert_eq!(exec_result.rows_affected, 0);

        let cursor_impl = connection.conn.execute(r#"SELECT A.NAME, A.ID, A.COLID, A.TYPE$, A.LENGTH$, A.SCALE, A.NULLABLE$, A.DEFVAL, A.INFO2 AS IS_IDENTITY, B.NAME AS TABLE_NAME, B.CRTDATE FROM SYSCOLUMNS AS a LEFT JOIN SYSOBJECTS AS B ON A.id = B.id WHERE B.name IN ('T4') AND B.SCHID IN (SELECT ID FROM SYSOBJECTS WHERE name = 'SYSDBA');"#, ()).unwrap().unwrap();
        cursor_impl.print_all_tables().unwrap();

        //2. query table

        let args: TableDescArgs<_, _> = (
            "SYSDBA",
            vec!["T2".to_string(), "T3".to_string(), "T4".to_string()],
        );
        let mut table_desc = connection.show_table(args).unwrap();

        let _: Vec<_> = table_desc
            .1
            .iter_mut()
            .map(|x| {
                let len = x.len();
                let id = x.get(1).unwrap().parse::<usize>().unwrap();
                // id must greater than 0
                assert!(id > 0);

                // validate CRTDATE value:2022-10-24 17:28:26.308000
                let crtdate = &x[len - 2];
                info!("{}", crtdate);
                assert!(validate_crtdate(crtdate));
                let _ = std::mem::replace(&mut x[1], "1058".to_string());
                let _ =
                    std::mem::replace(&mut x[len - 2], "2022-10-24 17:28:26.308000".to_string());
                x
            })
            .collect();

        // test Options case_sensitive:false
        info!("{}", serde_json::to_string(&table_desc).unwrap());
        assert_eq!(table_desc, mock_table_result());
    }

    pub fn mock_table_result() -> TableDescResult {
        let headers = svec![
            "NAME",
            "ID",
            "COLID",
            "TYPE$",
            "LENGTH$",
            "SCALE",
            "NULLABLE$",
            "DEFVAL",
            "IS_IDENTITY",
            "TABLE_NAME",
            "CRTDATE",
            "SUBTYPE$"
        ];

        let datas = vec![
            svec![
                "C1",
                "1058",
                "0",
                "DATETIME WITH TIME ZONE",
                "10",
                "6",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "C2",
                "1058",
                "1",
                "TIMESTAMP",
                "8",
                "6",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "C3",
                "1058",
                "2",
                "VARCHAR",
                "100",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "C4",
                "1058",
                "3",
                "NUMERIC",
                "0",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "C5",
                "1058",
                "4",
                "TIME WITH TIME ZONE",
                "7",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "C6",
                "1058",
                "5",
                "TIMESTAMP WITH LOCAL TIME ZONE",
                "8",
                "4102",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "NUMBER",
                "1058",
                "6",
                "NUMBER",
                "0",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "DECIMAL",
                "1058",
                "7",
                "DECIMAL",
                "0",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "BIT",
                "1058",
                "8",
                "BIT",
                "1",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "INTEGER",
                "1058",
                "9",
                "INTEGER",
                "4",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "XXX_PLS_INTEGER",
                "1058",
                "10",
                "INTEGER",
                "4",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "BIGINT",
                "1058",
                "11",
                "BIGINT",
                "8",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "TINYINT",
                "1058",
                "12",
                "TINYINT",
                "1",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "BYTE",
                "1058",
                "13",
                "BYTE",
                "1",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "SMALLINT",
                "1058",
                "14",
                "SMALLINT",
                "2",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "BINARY",
                "1058",
                "15",
                "BINARY",
                "1",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "VARBINARY",
                "1058",
                "16",
                "VARBINARY",
                "8188",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "REAL",
                "1058",
                "17",
                "REAL",
                "4",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "FLOAT",
                "1058",
                "18",
                "FLOAT",
                "8",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "DOUBLE",
                "1058",
                "19",
                "DOUBLE",
                "8",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "DOUBLE_PRECISION",
                "1058",
                "20",
                "DOUBLE PRECISION",
                "8",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "CHAR",
                "1058",
                "21",
                "CHAR",
                "1",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "VARCHAR",
                "1058",
                "22",
                "VARCHAR",
                "8188",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "TEXT",
                "1058",
                "23",
                "TEXT",
                "2147483647",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "IMAGE",
                "1058",
                "24",
                "IMAGE",
                "2147483647",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "BLOB",
                "1058",
                "25",
                "BLOB",
                "2147483647",
                "0",
                "Y",
                "",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "NOT_NULL_TEST",
                "1058",
                "26",
                "VARCHAR",
                "100",
                "0",
                "N",
                "'default_value_hh'",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "NOT_NULL_TEST_LEN",
                "1058",
                "27",
                "VARCHAR",
                "100",
                "0",
                "N",
                "'default_value_hh'",
                "0",
                "T2",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "C1",
                "1058",
                "0",
                "DATETIME WITH TIME ZONE",
                "10",
                "6",
                "Y",
                "",
                "0",
                "T3",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "CASE_SENSITIVE",
                "1058",
                "1",
                "TIMESTAMP",
                "8",
                "6",
                "Y",
                "",
                "0",
                "T3",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "C3",
                "1058",
                "2",
                "VARCHAR",
                "100",
                "0",
                "Y",
                "",
                "0",
                "T3",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "C4",
                "1058",
                "3",
                "NUMERIC",
                "0",
                "0",
                "Y",
                "",
                "0",
                "T3",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "NOT_NULL_TEST_LEN",
                "1058",
                "4",
                "VARCHAR",
                "100",
                "0",
                "N",
                "'default_value_hh'",
                "0",
                "T3",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "ID",
                "1058",
                "0",
                "INT",
                "4",
                "0",
                "N",
                "",
                "0",
                "T4",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "USER_ID",
                "1058",
                "1",
                "CHARACTER VARYING",
                "8188",
                "0",
                "N",
                "",
                "0",
                "T4",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "USER_NAME",
                "1058",
                "2",
                "TEXT",
                "2147483647",
                "0",
                "N",
                "",
                "0",
                "T4",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "ROLE",
                "1058",
                "3",
                "TEXT",
                "2147483647",
                "0",
                "N",
                "",
                "0",
                "T4",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
            svec![
                "SOURCE",
                "1058",
                "4",
                "TEXT",
                "2147483647",
                "0",
                "N",
                "",
                "0",
                "T4",
                "2022-10-24 17:28:26.308000",
                "UTAB"
            ],
        ];
        (headers, datas)
    }
}
