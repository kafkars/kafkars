//! Stable selection and exact transfer of the next driver-ready Produce owner.

use std::collections::HashSet;

use super::{
    PreparedExecution,
    handoff::{PreparedProduceHandoffError, PreparedProduceSubmission},
};

impl PreparedExecution {
    /// Transfers the longest ready admission-order prefix sharing one broker.
    pub(crate) fn take_next_driver_submissions(
        &mut self,
    ) -> Result<Vec<PreparedProduceSubmission>, PreparedProduceHandoffError> {
        let Some(first) = self
            .entries
            .values()
            .find(|entry| entry.submission.is_some())
        else {
            return Ok(Vec::new());
        };
        let broker = first.materialized.leader_broker_id();
        let deadline = first
            .submission
            .unwrap_or_else(|| unreachable!("selected Produce entry remains armed"))
            .deadline;
        let count = match broker {
            Some(broker) => self.broker_group_count(broker, deadline)?,
            None => 1,
        };
        let mut executions = Vec::new();
        executions
            .try_reserve_exact(count)
            .map_err(|_| PreparedProduceHandoffError::GroupingCapacity { requested: count })?;
        executions.extend(
            self.entries
                .values()
                .filter(|entry| entry.submission.is_some())
                .take(count)
                .map(|entry| entry.execution),
        );
        let next_retained_bytes = self.preflight_driver_submission_group(&executions)?;

        let mut submissions = Vec::new();
        submissions
            .try_reserve_exact(count)
            .map_err(|_| PreparedProduceHandoffError::GroupingCapacity { requested: count })?;
        for execution in executions {
            let entry = self
                .entries
                .remove(&execution.batch_id())
                .unwrap_or_else(|| unreachable!("preflighted Produce group entry remains owned"));
            let submission = entry
                .submission
                .unwrap_or_else(|| unreachable!("preflighted Produce group remains armed"));
            let scheduled = super::ScheduledDeadline {
                deadline: submission.deadline.core(),
                execution,
            };
            if !self.schedule.remove(&scheduled) {
                unreachable!("preflighted Produce group schedule cannot diverge");
            }
            submissions.push(PreparedProduceSubmission::new(
                execution,
                submission.deadline,
                entry.materialized,
            ));
        }
        self.retained_bytes = next_retained_bytes;
        Ok(submissions)
    }

    fn broker_group_count(
        &self,
        broker: i32,
        deadline: crate::clock::OperationDeadline,
    ) -> Result<usize, PreparedProduceHandoffError> {
        let requested = self.submission_count();
        let mut targets = HashSet::new();
        targets
            .try_reserve(requested)
            .map_err(|_| PreparedProduceHandoffError::GroupingCapacity { requested })?;
        let mut count = 0usize;
        let mut record_bytes = 0usize;
        for candidate in self
            .entries
            .values()
            .filter(|entry| entry.submission.is_some())
        {
            if candidate.materialized.leader_broker_id() != Some(broker) {
                break;
            }
            if candidate.submission.map(|submission| submission.deadline) != Some(deadline) {
                break;
            }
            if !targets.insert((
                candidate.materialized.topic_name(),
                candidate.materialized.partition(),
            )) {
                break;
            }
            let Some(next_bytes) =
                record_bytes.checked_add(candidate.materialized.retained_record_bytes())
            else {
                break;
            };
            if count > 0 && next_bytes > self.max_batch_bytes {
                break;
            }
            record_bytes = next_bytes;
            count = count.saturating_add(1);
        }
        Ok(count)
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
