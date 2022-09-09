use nu_protocol::Config;
use nu_table::{Alignments, Table};
use std::collections::HashMap;

mod error;
pub mod execute;
pub mod query;
pub mod database;

pub trait Print {
    fn print(&self) -> Result<(), &'static str> {
        let table = self.covert_table();
        let cfg = Config::default();
        let styles = HashMap::default();
        let alignments = Alignments::default();

        let p = table
            .draw_table(&cfg, &styles, alignments, usize::MAX)
            .ok_or("convert table to string error")?;
        debug!("\n{}", p);
        Ok(())
    }

    fn covert_table(&self) -> Table;
}
