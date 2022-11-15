use bytes::BytesMut;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

pub fn parse_to_bool(v: BytesMut) -> anyhow::Result<bool> {
    Ok(String::from_utf8_lossy(v.as_ref())
        .to_string()
        .parse::<bool>()?)
}

pub fn parse_to_i8(v: BytesMut) -> anyhow::Result<i8> {
    Ok(String::from_utf8_lossy(v.as_ref())
        .to_string()
        .parse::<i8>()?)
}

pub fn parse_to_int2(v: BytesMut) -> anyhow::Result<i16> {
    Ok(String::from_utf8_lossy(v.as_ref())
        .to_string()
        .parse::<i16>()?)
}

pub fn parse_to_int4(v: BytesMut) -> anyhow::Result<i32> {
    Ok(String::from_utf8_lossy(v.as_ref())
        .to_string()
        .parse::<i32>()?)
}

pub fn parse_to_int8(v: BytesMut) -> anyhow::Result<i64> {
    Ok(String::from_utf8_lossy(v.as_ref())
        .to_string()
        .parse::<i64>()?)
}

pub fn parse_to_float4(v: BytesMut) -> anyhow::Result<f32> {
    Ok(String::from_utf8_lossy(v.as_ref())
        .to_string()
        .parse::<f32>()?)
}

pub fn parse_to_float8(v: BytesMut) -> anyhow::Result<f64> {
    Ok(String::from_utf8_lossy(v.as_ref())
        .to_string()
        .parse::<f64>()?)
}

pub fn parse_to_string(v: BytesMut) -> String {
    String::from_utf8_lossy(v.as_ref()).to_string()
}

pub fn parse_to_date(v: BytesMut) -> anyhow::Result<NaiveDate> {
    let val = String::from_utf8_lossy(v.as_ref()).to_string();
    let date = NaiveDate::parse_from_str(val.as_str(), "%Y-%m-%d")?;
    Ok(date)
}

pub fn parse_to_time(v: BytesMut) -> anyhow::Result<NaiveTime> {
    let val = String::from_utf8_lossy(v.as_ref()).to_string();
    let time = NaiveTime::parse_from_str(
        val.as_str(),
        if val.contains('+') {
            "%H:%M:%S%.f%#z"
        } else {
            "%H:%M:%S%.f"
        },
    )
    .unwrap();
    Ok(time)
}

pub fn parse_to_data_time(v: BytesMut) -> anyhow::Result<NaiveDateTime> {
    let val = String::from_utf8_lossy(v.as_ref()).to_string();
    let date_time = NaiveDateTime::parse_from_str(
        val.as_str(),
        if val.contains('+') {
            "%Y-%m-%d %H:%M:%S%.f%#z"
        } else {
            "%Y-%m-%d %H:%M:%S%.f"
        },
    )?;
    Ok(date_time)
}
