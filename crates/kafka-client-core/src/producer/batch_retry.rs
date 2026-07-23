//! Sole batch-field mutation owner for retry attempt replacement.

use crate::{BatchExecutionGeneration, BatchTimerGeneration, Deadline};

use super::{BatchState, ProducerBatch};

impl ProducerBatch {
    pub(crate) fn commit_retry_waiting(
        &mut self,
        execution_generation: BatchExecutionGeneration,
        retries_started: u32,
        timer_generation: BatchTimerGeneration,
        timer_deadline: Deadline,
    ) {
        self.execution_generation = Some(execution_generation);
        self.retries_started = retries_started;
        self.timer_generation = timer_generation;
        self.timer_deadline = timer_deadline;
        self.state = BatchState::RetryWaiting;
    }
}
