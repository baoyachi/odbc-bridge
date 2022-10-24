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

    pub static ENV: Lazy<Environment> = Lazy::new(|| Environment::new().unwrap());

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
        // assert_eq!(exec_result.rows_affected, 1);
        println!("exec_result:{:?}",exec_result);

        let table_desc = connection.show_table(vec!["T2".to_string()]).unwrap();
        let json = serde_json::to_string(&table_desc).unwrap();
        println!("{}",json);
        assert_eq!(table_desc, (vec![], vec![]));
    }
}
