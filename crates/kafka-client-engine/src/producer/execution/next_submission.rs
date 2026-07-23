//! Stable selection and exact transfer of the next driver-ready Produce owner.

use super::{
    PreparedExecution,
    handoff::{PreparedProduceHandoffError, PreparedProduceSubmission},
};

impl PreparedExecution {
    /// Transfers the lowest armed `BatchId`.
    ///
    /// Core assigns batch identities monotonically and never reuses one, so
    /// ascending identity is the stable admission order for ready submissions.
    pub(crate) fn take_next_driver_submission(
        &mut self,
    ) -> Result<Option<PreparedProduceSubmission>, PreparedProduceHandoffError> {
        let Some(execution) = self.deadlines.next_handoff_execution() else {
            return Ok(None);
        };
        self.take_driver_submission(execution).map(Some)
    }
}
