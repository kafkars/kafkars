//! Atomic transfer of one freshly routed group after exact snapshot preflight.

use super::{PreparedProduceHandoffError, PreparedProduceSubmission};
use crate::producer::execution::{
    PreparedExecution, PreparedProduceError, PreparedProduceRouteCandidate,
    PreparedProduceRouteKey, ScheduledDeadline, next_submission::entry_matches_key,
};

impl PreparedExecution {
    /// Atomically detaches one selected broker group after revalidating every fact.
    pub(in crate::producer) fn take_driver_submission_group(
        &mut self,
        key: &PreparedProduceRouteKey,
        candidates: &[PreparedProduceRouteCandidate],
    ) -> Result<Vec<PreparedProduceSubmission>, PreparedProduceHandoffError> {
        let next_retained_bytes = self.preflight_driver_submission_group(key, candidates)?;
        let mut submissions = Vec::new();
        submissions
            .try_reserve_exact(candidates.len())
            .map_err(|_| PreparedProduceHandoffError::GroupingCapacity {
                requested: candidates.len(),
            })?;

        for candidate in candidates {
            let execution = candidate.execution();
            let entry = self
                .entries
                .remove(&execution.batch_id())
                .unwrap_or_else(|| unreachable!("preflighted Produce entry remains owned"));
            let submission = entry
                .submission
                .unwrap_or_else(|| unreachable!("preflighted Produce entry remains armed"));
            let scheduled = ScheduledDeadline {
                deadline: submission.deadline.core(),
                execution,
            };
            if !self.schedule.remove(&scheduled) {
                unreachable!("preflighted Produce schedule cannot diverge");
            }
            submissions.push(PreparedProduceSubmission::new(
                execution,
                submission.operation_id,
                submission.deadline,
                entry.materialized,
            ));
        }
        self.retained_bytes = next_retained_bytes;
        Ok(submissions)
    }

    fn preflight_driver_submission_group(
        &self,
        key: &PreparedProduceRouteKey,
        candidates: &[PreparedProduceRouteCandidate],
    ) -> Result<usize, PreparedProduceHandoffError> {
        let mut next_bytes = self.retained_bytes;
        for (index, candidate) in candidates.iter().enumerate() {
            let execution = candidate.execution();
            if candidates[..index]
                .iter()
                .any(|previous| previous.execution() == execution)
            {
                return Err(PreparedProduceHandoffError::RouteSnapshotMismatch { execution });
            }
            let (submission, _scheduled, _individual_next_bytes) =
                self.preflight_driver_submission(execution)?;
            let retained = self
                .entries
                .get(&execution.batch_id())
                .unwrap_or_else(|| unreachable!("preflighted Produce entry remains retained"));
            if submission.operation_id != candidate.operation_id()
                || retained.materialized.partition() != candidate.partition()
                || !entry_matches_key(retained, key)
            {
                return Err(PreparedProduceHandoffError::RouteSnapshotMismatch { execution });
            }
            next_bytes = next_bytes
                .checked_sub(retained.materialized.retained_record_bytes())
                .ok_or(PreparedProduceHandoffError::AccountingInconsistent {
                    execution,
                    deadline: submission.deadline,
                    reason: PreparedProduceError::EncodedByteOverflow,
                })?;
        }
        Ok(next_bytes)
    }
}
