#[macro_use]
extern crate anyhow;

#[macro_use]
extern crate log;

pub use odbc_api;

pub mod brige;
pub mod debug;
pub mod executor;
pub mod extension;

pub use debug::print_all_tables;
pub use dameng_helper::*;

pub trait Convert<T>: Sized {
    fn convert(self) -> T;
}

pub trait TryConvert<T>: Sized {
    type Error;
    fn try_convert(self) -> Result<T, Self::Error>;
}
