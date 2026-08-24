//! Machine-level join for exact Fetch success, failure, and throttle facts.

use super::{
    AssignedConsumerMachine, AssignedConsumerMachineError, AssignedConsumerTransition,
    FetchFailure, FetchFence, FetchOwnership, FetchRecords, NextFetchOffset,
};
use crate::Moment;

impl AssignedConsumerMachine {
    /// Reports whether one exact Fetch still owns the active partition execution.
    ///
    /// Interpreters use this query before acquiring bytes or transport
    /// ownership for work that may have waited outside the machine.
    pub fn fetch_ownership(
        &self,
        fence: FetchFence,
    ) -> Result<FetchOwnership, AssignedConsumerMachineError> {
        let position = fence.position();
        let assignment = self
            .assignment
            .as_ref()
            .ok_or(AssignedConsumerMachineError::NoAssignment)?;
        match assignment.find(position.partition()) {
            Some(state) => state.fetch_ownership(fence),
            None if position.assignment_epoch() < assignment.epoch => {
                Ok(FetchOwnership::Superseded)
            }
            None if position.assignment_epoch() > assignment.epoch => {
                Err(AssignedConsumerMachineError::StaleAssignment {
                    active: assignment.epoch,
                    supplied: position.assignment_epoch(),
                })
            }
            None => Err(AssignedConsumerMachineError::UnknownPartition {
                partition: position.partition(),
            }),
        }
    }

    pub(super) fn fetch_advanced(
        &mut self,
        fence: FetchFence,
        records: FetchRecords,
        next_offset: NextFetchOffset,
        now: Moment,
        throttle_ticks: u64,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        let position = fence.position();
        let (current, state) =
            self.fenced_partition_mut(position.assignment_epoch(), position.partition())?;
        let effects = state.fetch_advanced(fence, records, next_offset, now, throttle_ticks)?;
        Ok(AssignedConsumerTransition::new(current, effects))
    }

    pub(super) fn fetch_failed(
        &mut self,
        fence: FetchFence,
        failure: FetchFailure,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        let position = fence.position();
        let (current, state) =
            self.fenced_partition_mut(position.assignment_epoch(), position.partition())?;
        let effect = state.fetch_failed(fence, failure)?;
        Ok(AssignedConsumerTransition::new(current, vec![effect]))
    }

    pub(super) fn fetch_retry(
        &mut self,
        fence: FetchFence,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        let position = fence.position();
        let (current, state) =
            self.fenced_partition_mut(position.assignment_epoch(), position.partition())?;
        let effect = state.fetch_retry(fence)?;
        Ok(AssignedConsumerTransition::new(current, vec![effect]))
    }

    pub(super) fn fetch_throttle_elapsed(
        &mut self,
        fence: FetchFence,
        now: Moment,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        let position = fence.position();
        let (current, state) =
            self.fenced_partition_mut(position.assignment_epoch(), position.partition())?;
        let effect = state.fetch_throttle_elapsed(fence, now)?;
        Ok(AssignedConsumerTransition::new(current, vec![effect]))
    }
}
