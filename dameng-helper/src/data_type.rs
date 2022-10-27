use crate::error::DmError;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub enum DataType {
    /// `NUMERIC 数据类型用于存储零、正负定点数。其中:精度是一个无符号整数，
    /// 定义 了总的数字数，精度范围是 1至38，标度定义了小数点右边的数字位数，定义时如省略 精度，则默认是 16。
    /// 如省略标度，则默认是 0。一个数的标度不应大于其精度。例如: NUMERIC(4,1)定义了小数点前面 3 位和小数点后面 1 位，共 4 位的数字，范围在 - 999.9 到 999.9。
    /// 所有 NUMERIC 数据类型，如果其值超过精度，达梦数据库返回一个 出错信息，如果超过标度，则多余的位截断。
    /// 如果不指定精度和标度，缺省精度为 38。
    /// NUMERIC[( 精度 [, 标度])]
    NUMERIC,

    /// 与 NUMERIC 类型相同。
    /// NUMBER[(精度 [, 标度])]
    NUMBER,

    /// 与 NUMERIC 相似。
    /// DECIMAL[(精度 [, 标度])],
    DECIMAL,

    /// BIT 类型用于存储整数数据 1、0 或 NULL，可以用来支持 ODBC 和 JDBC 的布尔数据 类型。DM 的 BIT 类型与 SQL SERVER2000 的 BIT 数据类型相似。
    BIT,

    /// 用于存储有符号整数，精度为 10，标度为 0。取值范围为:-2147483648(- 2^31)~ +2147483647(2^31-1)。
    INTEGER,

    /// 用于存储有符号整数，精度为 19，标度为 0。取值范围为:-9223372036854775808(-2^63)~ 9223372036854775807(2^63-1)。
    BIGINT,

    /// 用于存储有符号整数，精度为 3，标度为 0。取值范围为:-128~+127。
    TINYINT,

    /// 与 TINYINT 相似，精度为 3，标度为 0。
    BYTE,
    /// 用于存储有符号整数，精度为 5，标度为 0。
    SMALLINT,
    /// BINARY 数据类型指定定长二进制数据。缺省长度为 1 个字节，最大长度由数据库页 面大小决定，具体可参考《DM8_SQL 语言使用手册》1.4.1 节。BINARY 常量以 0x 开始， 后跟数据的十六进制表示，例如 0x2A3B4058。
    BINARY,
    /// VARBINARY 数据类型指定变长二进制数据，用法类似 BINARY 数据类型，可以指定
    /// 一个正整数作为数据长度。缺省长度为 8188 个字节，最大长度由数据库页面大小决定，
    /// 具体可参考《DM8_SQL 语言使用手册》1.4.1 节。
    VARBINARY,

    /// REAL 是带二进制的浮点数，但它不能由用户指定使用的精度，系统指定其二进制精 度为24，十进制精度为7。取值范围-3.4E + 38 ~ 3.4E + 38。
    REAL,
    /// FLOAT 是带二进制精度的浮点数，精度最大不超过 53，如省略精度，则二进制精度 为53，十进制精度为15。取值范围为-1.7E + 308 ~ 1.7E + 308。
    FLOAT,
    /// 同 FLOAT 相似，精度最大不超过 53。
    DOUBLE,
    /// 该类型指明双精度浮点数，其二进制精度为 53，十进制精度为 15。取值范围-1.7E + 308 ~ 1.7E + 308。
    DOUBLE_PRECISION,
    /// 定长字符串，最大长度由数据库页面大小决定，具体可参考《DM8_SQL 语言使用手册》 1.4.1 节。长度不足时，自动填充空格。
    CHAR,
    /// 可变长字符串，最大长度由数据库页面大小决定，具体可参考《DM8_SQL 语言使用手 册》1.4.1 节。
    VARCHAR,
    /// 变长字符串类型，其字符串的长度最大为 2G-1，可用于存储长的文本串。
    TEXT,
    /// 可用于存储多媒体信息中的图像类型。图像由不定长的象素点阵组成，长度最大为2G-1 字节。该类型除了存储图像数据之外，还可用于存储任何其它二进制数据。
    IMAGE,
    /// BLOB 类型用于指明变长的二进制大对象，长度最大为 2G-1 字节。
    BLOB,
    /// CLOB 类型用于指明变长的字符串，长度最大为 2G-1 字节。
    CLOB,
    /// BFILE 用于指明存储在操作系统中的二进制文件，文件存储在操作系统而非数据库中， 仅能进行只读访问。
    BFILE,
    /// DATE 类型包括年、月、日信息，定义了'-4712-01-01'和'9999-12-31'之间任 何一个有效的格里高利日期。
    DATE,
    /// TIME 类型包括时、分、秒信息，定义了一个在'00:00:00.000000'和 '23:59:59.999999'之间的有效时间。TIME 类型的小数秒精度规定了秒字段中小数点 后面的位数，取值范围为 0~6，如果未定义，缺省精度为 0。
    TIME,
    /// TIMESTAMP/DATETIME 类型包括年、月、日、时、分、秒信息，定义了一个在'- 4712-01-01 00:00:00.000000'和'9999-12-31 23:59:59.999999'之间的有效 格里高利日期时间。小数秒精度规定了秒字段中小数点后面的位数，取值范围为 0~6，如 果未定义，缺省精度为 6。
    TIMESTAMP,
    /// 描述一个带时区的 TIME 值，其定义是在 TIME 类型的后面加上时区信息。时区部分 的实质是 INTERVAL HOUR TO MINUTE 类型，取值范围:-12:59 与+14:00 之间。例 如:TIME '09:10:21 +8:00'
    TIME_WITH_TIME_ZONE,
    /// 描述一个带时区的 TIMESTAMP 值，其定义是在 TIMESTAMP 类型的后面加上时区信 息。时区部分的实质是 INTERVAL HOUR TO MINUTE 类型，取值范围:-12:59 与 +14:00 之间。例如:’2009-10-11 19:03:05.0000 -02:10’。
    TIMESTAMP_WITH_TIME_ZONE,
    /// 描述一个本地时区的TIMESTAMP值，能够将标准时区类型TIMESTAMP WITH TIME ZONE 类型转化为本地时区类型，如果插入的值没有指定时区，则默认为本地时区。
    TIMESTAMP_WITH_LOCAL_TIME_ZONE,

    /// 布尔数据类型:TRUE 和 FALSE。DMSQL 程序的布尔类型和 INT 类型可以相互转化。 如果变量或方法返回的类型是布尔类型，则返回值为 0 或 1。TRUE 和非 0 值的返回值为 1，FALSE 和 0 值返回为 0。
    BOOL,
    // TODO 时间间隔数据类型

    //未知类型
    Unknown,
}

impl Default for DataType {
    fn default() -> Self {
        Self::Unknown
    }
}

impl FromStr for DataType {
    type Err = DmError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let data_type = match &*s.to_uppercase() {
            "NUMERIC" => Self::NUMERIC,
            "NUMBER" => Self::NUMBER,
            "DECIMAL" => Self::DECIMAL,
            "BIT" => Self::BIT,
            "INT" | "INTEGER" | "PLS_INTEGER" => Self::INTEGER,
            "BIGINT" => Self::BIGINT,
            "TINYINT" => Self::TINYINT,
            "BYTE" => Self::BYTE,
            "SMALLINT" => Self::SMALLINT,
            "BINARY" => Self::BINARY,
            "VARBINARY" => Self::VARBINARY,
            "REAL" => Self::REAL,
            "FLOAT" => Self::FLOAT,
            "DOUBLE" => Self::DOUBLE,
            "DOUBLE PRECISION" => Self::DOUBLE_PRECISION,
            "CHAR" => Self::CHAR,
            "VARCHAR" | "CHARACTER VARYING" => Self::VARCHAR,
            "TEXT" => Self::TEXT,
            "IMAGE" => Self::IMAGE,
            "BLOB" => Self::BLOB,
            "CLOB" => Self::CLOB,
            "BFILE" => Self::BFILE,
            "DATE" => Self::DATE,
            "TIME" => Self::TIME,
            "TIMESTAMP" => Self::TIMESTAMP,
            "TIME WITH TIME ZONE" => Self::TIME_WITH_TIME_ZONE,
            "DATETIME WITH TIME ZONE" => Self::TIMESTAMP_WITH_TIME_ZONE,
            "TIMESTAMP WITH LOCAL TIME ZONE" => Self::TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            _ => return Err(DmError::DataTypeError(s.to_string())),
        };
        Ok(data_type)
    }
}

pub trait TryToString {
    type Err;
    fn try_to_string(&self) -> Result<String, Self::Err>;
}

impl TryToString for DataType {
    type Err = DmError;

    fn try_to_string(&self) -> Result<String, Self::Err> {
        match self {
            DataType::NUMERIC => Ok("NUMERIC".to_string()),
            DataType::NUMBER => Ok("NUMBER".to_string()),
            DataType::DECIMAL => Ok("DECIMAL".to_string()),
            DataType::BIT => Ok("BIT".to_string()),
            DataType::INTEGER => Ok("INT".to_string()),
            DataType::BIGINT => Ok("BIGINT".to_string()),
            DataType::TINYINT => Ok("TINYINT".to_string()),
            DataType::BYTE => Ok("BYTE".to_string()),
            DataType::SMALLINT => Ok("SMALLINT".to_string()),
            DataType::BINARY => Ok("BINARY".to_string()),
            DataType::VARBINARY => Ok("VARBINARY".to_string()),
            DataType::REAL => Ok("REAL".to_string()),
            DataType::FLOAT => Ok("FLOAT".to_string()),
            DataType::DOUBLE => Ok("DOUBLE".to_string()),
            DataType::DOUBLE_PRECISION => Ok("DOUBLE PRECISION".to_string()),
            DataType::CHAR => Ok("CHAR".to_string()),
            DataType::VARCHAR => Ok("VARCHAR".to_string()),
            DataType::TEXT => Ok("TEXT".to_string()),
            DataType::IMAGE => Ok("IMAGE".to_string()),
            DataType::BLOB => Ok("BLOB".to_string()),
            DataType::CLOB => Ok("CLOB".to_string()),
            DataType::BFILE => Ok("BFILE".to_string()),
            DataType::DATE => Ok("DATE".to_string()),
            DataType::TIME => Ok("TIME".to_string()),
            DataType::TIMESTAMP => Ok("TIMESTAMP".to_string()),
            DataType::TIME_WITH_TIME_ZONE => Ok("TIME WITH TIME ZONE".to_string()),
            DataType::TIMESTAMP_WITH_TIME_ZONE => Ok("DATETIME WITH TIME ZONE".to_string()),
            DataType::TIMESTAMP_WITH_LOCAL_TIME_ZONE => {
                Ok("TIMESTAMP WITH LOCAL TIME ZONE".to_string())
            }
            _ => return Err(DmError::DataTypeError(format!("{:?}", self))),
        }
    }
}
