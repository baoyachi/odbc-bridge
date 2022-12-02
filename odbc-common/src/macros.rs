#[macro_export]
macro_rules! sqlstate_mapping {
    (
        $objectname:ident,
        $(
            $(#[$docs:meta])*
            ($phrase:ident, $state_pg:expr);
        )+
    ) => {
        use serde::*;

        #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
        #[allow(non_upper_case_globals)]
        #[allow(non_camel_case_types)]
        pub enum $objectname {
            $(
                #[serde(rename = $state_pg)]
                $phrase,
            )+
        }
        impl std::fmt::Display for $objectname {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(
                        Self::$phrase => write!(f, $state_pg),
                    )+
                }
            }
        }

        #[allow(unreachable_patterns)]
        pub fn get_obj_by_state(state: &str) -> Option<$objectname> {
            match state {
                $(
                    $state_pg => Some($objectname::$phrase),
                )+
                _ => None
            }
        }
    }
}
