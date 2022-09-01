#![allow(dead_code)]
#[macro_use]
extern crate anyhow;

#[macro_use]
extern crate log;

pub use odbc_api;

pub mod debug;
pub mod executor;

pub use debug::print_all_tables;
