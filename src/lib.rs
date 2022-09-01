#![allow(dead_code)]
#[macro_use]
extern crate anyhow;

#[macro_use]
extern crate log;

pub mod debug;
mod executor;

pub use debug::print_all_tables;
