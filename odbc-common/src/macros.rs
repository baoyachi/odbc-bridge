#[macro_export]
macro_rules! sqlstate_mapping {
    (
        $obj:ident,
        $(
            $(#[$docs:meta])*
            ($phrase:ident, $state:expr);
        )+
    ) => {
        use serde::*;

        #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
        #[allow(non_upper_case_globals)]
        #[allow(non_camel_case_types)]
        pub enum $obj {
            $(
                #[serde(rename = $state)]
                $phrase,
            )+
        }
        impl std::fmt::Display for $obj {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(
                        Self::$phrase => write!(f, $state),
                    )+
                }
            }
        }

        #[allow(unreachable_patterns)]
        pub fn get_obj_by_state(state: &str) -> Option<$obj> {
            match state {
                $(
                    $state => Some($obj::$phrase),
                )+
                _ => None
            }
        }
    }
}
