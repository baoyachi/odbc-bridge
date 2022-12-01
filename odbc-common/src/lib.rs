#[macro_use]
extern crate log;

pub extern crate odbc_api;

pub mod error;
pub mod print_table;
pub mod sqlstate_handler;
pub use nu_protocol::*;
pub use nu_table::*;

pub use print_table::Print;

#[macro_export]
macro_rules! sqlstate_mapping {
    (
        $objectname:ident,
        $(
            $(#[$docs:meta])*
            ($phrase:ident, $state_pg:expr);
        )+
    ) => {
        #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
        #[allow(non_upper_case_globals)]
        #[allow(non_camel_case_types)]
        pub enum $objectname {
            $(
                #[serde(rename = $state_pg)]
                $phrase,
            )+
        }
        impl fmt::Display for $objectname {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(
                        Self::$phrase => write!(f, $state_pg),
                    )+
                }
            }
        }

        pub fn get_obj_by_state(state: &str) -> Option<$objectname> {
            match state {
                $(
                    #[allow(unreachable_patterns)]
                    $state_pg => Some($objectname::$phrase),
                )+
                _ => None
            }
        }
    }
}
