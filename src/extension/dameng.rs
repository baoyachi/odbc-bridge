use dameng_helper::table::DmTableDesc;
use crate::executor::table::TableDescResult;
use crate::TryConvert;

impl TryConvert<DmTableDesc> for TableDescResult{
    type Error = anyhow::Error;

    fn try_convert(self) -> Result<DmTableDesc, Self::Error> {
        DmTableDesc::new(self.0,self.1)
    }
}