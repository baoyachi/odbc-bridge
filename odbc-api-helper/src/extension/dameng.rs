use crate::executor::table::TableDescResult;
use crate::TryConvert;
use dameng_helper::table::DmTableDesc;
use odbc_common::error::{OdbcStdError, OdbcStdResult};

impl TryConvert<DmTableDesc> for TableDescResult {
    type Error = OdbcStdError;

    fn try_convert(self) -> OdbcStdResult<DmTableDesc, Self::Error> {
        DmTableDesc::new(self.0, self.1)
    }
}
