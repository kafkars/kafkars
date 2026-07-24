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
        if position.assignment_epoch() < assignment.epoch {
            return Ok(FetchOwnership::Superseded);
        }
        if position.assignment_epoch() > assignment.epoch {
            return Err(AssignedConsumerMachineError::StaleAssignment {
                active: assignment.epoch,
                supplied: position.assignment_epoch(),
            });
        }
        assignment
            .partitions
            .iter()
            .find(|state| state.partition == position.partition())
            .ok_or(AssignedConsumerMachineError::UnknownPartition {
                partition: position.partition(),
            })?
            .fetch_ownership(fence)
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
        let effects = self
            .assignment_mut(position.assignment_epoch())?
            .find_mut(position.partition())?
            .fetch_advanced(fence, records, next_offset, now, throttle_ticks)?;
        Ok(AssignedConsumerTransition::new(
            position.assignment_epoch(),
            effects,
        ))
    }

    pub(super) fn fetch_failed(
        &mut self,
        fence: FetchFence,
        failure: FetchFailure,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        let position = fence.position();
        let effect = self
            .assignment_mut(position.assignment_epoch())?
            .find_mut(position.partition())?
            .fetch_failed(fence, failure)?;
        Ok(AssignedConsumerTransition::new(
            position.assignment_epoch(),
            vec![effect],
        ))
    }

    pub(super) fn fetch_throttle_elapsed(
        &mut self,
        fence: FetchFence,
        now: Moment,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        let position = fence.position();
        let effect = self
            .assignment_mut(position.assignment_epoch())?
            .find_mut(position.partition())?
            .fetch_throttle_elapsed(fence, now)?;
        Ok(AssignedConsumerTransition::new(
            position.assignment_epoch(),
            vec![effect],
        ))
    }
}
