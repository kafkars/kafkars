//! Machine-level join for exact Fetch success, failure, and throttle facts.

use super::{
    AssignedConsumerMachine, AssignedConsumerMachineError, AssignedConsumerTransition,
    FetchFailure, FetchFence, FetchRecords, NextFetchOffset,
};
use crate::Moment;

impl AssignedConsumerMachine {
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
