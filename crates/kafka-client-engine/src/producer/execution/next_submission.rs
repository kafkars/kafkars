//! Stable selection and exact transfer of the next driver-ready Produce owner.

use super::{
    PreparedExecution,
    handoff::{PreparedProduceHandoffError, PreparedProduceSubmission},
};

impl PreparedExecution {
    /// Transfers one name-routed submission in stable admission order.
    pub(crate) fn take_next_driver_submissions(
        &mut self,
    ) -> Result<Vec<PreparedProduceSubmission>, PreparedProduceHandoffError> {
        let Some(execution) = self
            .entries
            .values()
            .find(|entry| entry.submission.is_some())
            .map(|entry| entry.execution)
        else {
            return Ok(Vec::new());
        };
        let mut submissions = Vec::new();
        submissions
            .try_reserve_exact(1)
            .map_err(|_| PreparedProduceHandoffError::GroupingCapacity { requested: 1 })?;
        submissions.push(self.take_driver_submission(execution)?);
        Ok(submissions)
    }

    /// Transfers the lowest armed `BatchId`.
    ///
    /// Core assigns batch identities monotonically and never reuses one, so
    /// ascending identity is the stable admission order for ready submissions.
    #[cfg(test)]
    pub(crate) fn take_next_driver_submission(
        &mut self,
    ) -> Result<Option<PreparedProduceSubmission>, PreparedProduceHandoffError> {
        let Some(execution) = self
            .entries
            .values()
            .find(|entry| entry.submission.is_some())
            .map(|entry| entry.execution)
        else {
            return Ok(None);
        };
        self.take_driver_submission(execution).map(Some)
    }
}
