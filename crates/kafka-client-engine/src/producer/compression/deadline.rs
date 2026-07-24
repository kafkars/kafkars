//! Exact cancellation and public-deadline fencing for retained compression jobs.

use kafka_client_core::{BatchExecutionId, BatchId, Deadline, Moment, OperationId, ProducerInput};

use super::CompressionWorkers;

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub(super) struct ScheduledDeadline {
    pub(super) deadline: Deadline,
    pub(super) execution: BatchExecutionId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct InFlight {
    pub(super) deadline_operation_id: OperationId,
    pub(super) deadline: Deadline,
    pub(super) reservation_bytes: usize,
    pub(super) cancelled: bool,
}

impl CompressionWorkers {
    pub(crate) fn cancel(&mut self, execution: BatchExecutionId) {
        if let Some(entry) = self.entries.get_mut(&execution) {
            entry.cancelled = true;
            self.schedule.remove(&ScheduledDeadline {
                deadline: entry.deadline,
                execution,
            });
        }
    }

    pub(crate) fn cancel_batch(&mut self, batch_id: BatchId) {
        let executions = self
            .entries
            .keys()
            .copied()
            .filter(|execution| execution.batch_id() == batch_id)
            .collect::<Vec<_>>();
        for execution in executions {
            self.cancel(execution);
        }
    }

    pub(crate) fn contains(&self, execution: BatchExecutionId) -> bool {
        self.entries
            .get(&execution)
            .is_some_and(|entry| !entry.cancelled)
    }

    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        self.schedule.first().map(|scheduled| scheduled.deadline)
    }

    pub(crate) fn drain_due(&mut self, now: Moment, limit: usize) -> Vec<ProducerInput> {
        let due = self
            .schedule
            .iter()
            .copied()
            .take_while(|scheduled| scheduled.deadline.is_elapsed_at(now))
            .take(limit)
            .collect::<Vec<_>>();
        let mut inputs = Vec::with_capacity(due.len());
        for scheduled in due {
            self.schedule.remove(&scheduled);
            if let Some(entry) = self.entries.get_mut(&scheduled.execution) {
                entry.cancelled = true;
                inputs.push(ProducerInput::DeadlineElapsed {
                    operation_id: entry.deadline_operation_id,
                    now,
                });
            }
        }
        inputs
    }
}
