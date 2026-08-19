//! Test-only observations of retained `DeleteRecords` call ownership.

use super::super::{DeleteRecordsHost, DeleteRecordsHostError};

impl DeleteRecordsHost {
    pub(in crate::admin::delete_records) fn retain_recovered_call_for_test(&mut self) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredDeleteRecordsCall::for_test());
    }

    pub(in crate::admin::delete_records) fn recovered_call_is_retained_for_test(&self) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::delete_records) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), DeleteRecordsHostError> {
        self.settle_recovered_transport(0)
    }
}
