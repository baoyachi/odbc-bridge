#![deny(missing_debug_implementations)]

#[macro_use]
extern crate log;

pub extern crate dameng_helper;

pub extern crate pg_helper;
#[macro_use]
extern crate serde;
pub extern crate odbc_common;

pub mod bridge;
pub mod executor;
pub mod extension;
pub mod state;

pub use odbc_common::Print;

pub trait Convert<T>: Sized {
    fn convert(self) -> T;
}

pub trait TryConvert<T>: Sized {
    type Error;
    fn try_convert(self) -> Result<T, Self::Error>;
}

#[macro_export]
macro_rules! sqlstate_try_convert {
    (
        $source:ident,
        $dest:ident,
        $(
            $(#[$docs:meta])*
            ($phrase:ident);
        )+
    ) => {
        impl TryConvert<$dest> for $source {
            type Error = OdbcStdError;

            fn try_convert(self) -> Result<$dest, Self::Error> {
                match self {
                    $(
                        $source::$phrase => Ok($dest::$phrase),
                    )+
                }
            }
        }

        impl TryConvert<$source> for $dest {
            type Error = OdbcStdError;

            fn try_convert(self) -> Result<$source, Self::Error> {
                match self {
                    $(
                        $dest::$phrase => Ok($source::$phrase),
                    )+
                }
            }
        }
    }
}
