use crate::extension::odbc::{OdbcColumnDesc, OdbcParamDesc};
use odbc_common::odbc_api::Prepared;

#[allow(missing_debug_implementations)]
pub struct OdbcPrepared<S> {
    pub prepared: Prepared<S>,
    pub result_cols_desc: Vec<OdbcColumnDesc>,
    pub params_desc: Vec<OdbcParamDesc>,
}

impl<S> OdbcPrepared<S> {
    pub fn result_cols_description(&self) -> &[OdbcColumnDesc] {
        &self.result_cols_desc
    }
    pub fn params_description(&self) -> &[OdbcParamDesc] {
        &self.params_desc
    }
}

impl<S> OdbcPrepared<S> {
    pub fn new(
        prepared: Prepared<S>,
        result_cols_des: Vec<OdbcColumnDesc>,
        params_des: Vec<OdbcParamDesc>,
    ) -> Self {
        Self {
            prepared,
            result_cols_desc: result_cols_des,
            params_desc: params_des,
        }
    }
}
