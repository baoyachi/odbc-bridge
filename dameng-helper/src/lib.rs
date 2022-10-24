#![deny(missing_debug_implementations)]

#[allow(non_camel_case_types)]
pub mod data_type;
pub mod error;
pub mod table;

pub use data_type::*;
use odbc_api::buffers::TextRowSet;
use odbc_api::handles::StatementImpl;
use odbc_api::{Cursor, CursorImpl, ResultSetMetadata};

pub trait DmAdapter {
    fn get_table_sql(table_names: Vec<String>, db_name: &str) -> String;
    fn get_table_desc(
        self,
        case_sensitive: bool,
    ) -> anyhow::Result<(Vec<String>, Vec<Vec<String>>)>;
}

impl DmAdapter for CursorImpl<StatementImpl<'_>> {
    fn get_table_sql(table_names: Vec<String>, db_name: &str) -> String {
        // Use sql: `SELECT A.*, B.NAME AS TABLE_NAME FROM SYSCOLUMNS AS a LEFT JOIN SYSOBJECTS AS B ON A.id = B.id WHERE B.name IN ("X")`;
        // The X is table name;
        let tables = table_names
            .iter()
            .map(|x| format!("'{}'", x))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            r#"SELECT A.NAME, A.ID, A.COLID, A.TYPE$, A.LENGTH$, A.SCALE, A.NULLABLE$, A.DEFVAL, B.NAME AS TABLE_NAME, B.CRTDATE FROM SYSCOLUMNS AS a LEFT JOIN SYSOBJECTS AS B ON A.id = B.id WHERE B.name IN ({}) AND B.SCHID IN (SELECT ID FROM SYSOBJECTS WHERE name = '{}');"#,
            tables, db_name
        )
    }

    fn get_table_desc(
        mut self,
        case_sensitive: bool,
    ) -> anyhow::Result<(Vec<String>, Vec<Vec<String>>)> {
        let headers = self.column_names()?.collect::<Result<Vec<_>, _>>()?;

        let mut buffers = TextRowSet::for_cursor(1024, &mut self, Some(4096))?;
        let mut row_set_cursor = self.bind_buffer(&mut buffers)?;

        let mut data = vec![];
        while let Some(batch) = row_set_cursor.fetch()? {
            for row_index in 0..batch.num_rows() {
                let num_cols = batch.num_cols();
                let row_data: Vec<_> = (0..num_cols)
                    .map(|col_index| batch.at(col_index, row_index).unwrap_or(&[]))
                    .into_iter()
                    .map(String::from_utf8_lossy)
                    .map(|x| {
                        if case_sensitive {
                            x.to_string()
                        } else {
                            x.to_uppercase()
                        }
                    })
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

    use odbc_api::Environment;
    use odbc_api_helper::executor::database::{ConnectionTrait, OdbcDbConnection, Options};
    use odbc_api_helper::executor::execute::ExecResult;
    use odbc_api_helper::executor::SupportDatabase;
    use once_cell::sync::Lazy;
    use regex::Regex;

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

        let connection = OdbcDbConnection::new(
            conn,
            Options::new("SYSDBA".to_string(), SupportDatabase::Dameng),
        )
        .unwrap();
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
        odbc_api_helper::print_all_tables(cursor).unwrap();
    }

    #[test]
    fn test_dameng_table_desc() {
        //1. create table
        //2. query table
        let create_table_sql = r#"
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
        let connection = get_dameng_conn();

        let exec_result: ExecResult = connection.execute(create_table_sql).unwrap();
        assert_eq!(exec_result.rows_affected, 0);

        let mut table_desc = connection.show_table(vec!["T2".to_string()]).unwrap();

        assert_eq!(
            table_desc.0,
            vec![
                "NAME",
                "ID",
                "COLID",
                "TYPE$",
                "LENGTH$",
                "SCALE",
                "NULLABLE$",
                "DEFVAL",
                "TABLE_NAME",
                "CRTDATE"
            ],
        );

        let _: Vec<_> = table_desc
            .1
            .iter_mut()
            .map(|x| {
                // id must greater than 0
                let id = x.get(1).unwrap().parse::<usize>().unwrap();
                assert!(id > 0);

                // validate CRTDATE value:2022-10-24 17:28:26.308000
                let crtdate = x.last().unwrap();
                assert_eq!(true, validate_crtdate(crtdate));
                x.insert(1, "1058".to_string());
                x.insert(x.len() - 1, "2022-10-24 17:28:26.308000".to_string());
                x
            })
            .collect();
        let table_rows = table_desc.1;
        println!("{}", serde_json::to_string(&table_rows).unwrap());

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

        assert_eq!(
            table_rows,
            vec![
                svec![
                    "C1",
                    "1058",
                    "0",
                    "DATETIME WITH TIME ZONE",
                    "10",
                    "6",
                    "Y",
                    "",
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000"
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
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
                    "T2",
                    "2022-10-24 17:28:26.308000"
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
                    "T2",
                    "2022-10-24 17:28:26.308000",
                ],
            ]
        );
    }
}
