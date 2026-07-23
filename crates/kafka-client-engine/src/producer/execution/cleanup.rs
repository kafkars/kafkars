//! Preflighted exact cleanup of batch membership, prepared bytes, and deadlines.

use kafka_client_core::BatchId;

use super::{
    PreparedExecution, PreparedExecutionError, PreparedProduceError, ScheduledDeadline,
    SubmissionDeadlineError,
};
use crate::producer::store::ProducerStore;

impl PreparedExecution {
    /// Releases every owner only after exact-generation preflight succeeds.
    pub(crate) fn release_batch(
        &mut self,
        store: &mut ProducerStore,
        batch_id: BatchId,
    ) -> Result<(), PreparedExecutionError> {
        let expected = store
            .batch_execution(batch_id)
            .map_err(PreparedExecutionError::Store)?;
        let retained = self.entries.get(&batch_id).map(|entry| entry.execution);
        if retained.is_some_and(|actual| Some(actual) != expected) {
            return Err(PreparedExecutionError::CleanupExecutionMismatch {
                batch_id,
                expected,
                retained,
            });
        }
        let release = expected
            .and_then(|execution| self.entries.get(&batch_id).map(|entry| (execution, entry)))
            .map(|(execution, entry)| {
                let next_bytes = self
                    .retained_bytes
                    .checked_sub(entry.materialized.retained_record_bytes())
                    .ok_or(PreparedExecutionError::Prepared(
                        PreparedProduceError::EncodedByteOverflow,
                    ))?;
                let scheduled = entry.submission.map(|submission| ScheduledDeadline {
                    deadline: submission.deadline.core(),
                    execution,
                });
                if scheduled.is_some_and(|deadline| !self.schedule.contains(&deadline)) {
                    return Err(PreparedExecutionError::Deadline(
                        SubmissionDeadlineError::ConflictingBatch { batch_id },
                    ));
                }
                Ok((next_bytes, scheduled))
            })
            .transpose()?;
        store
            .release_batch(batch_id)
            .map_err(PreparedExecutionError::Store)?;
        if let Some((next_bytes, scheduled)) = release {
            if let Some(deadline) = scheduled {
                self.schedule.remove(&deadline);
            }
            self.entries.remove(&batch_id);
            self.retained_bytes = next_bytes;
        }
        Ok(())
    }

    /// Drops all execution mechanisms during outside-in terminal recovery.
    pub(crate) fn clear_terminal(&mut self) {
        self.entries.clear();
        self.schedule.clear();
        self.retained_bytes = 0;
    }
}
