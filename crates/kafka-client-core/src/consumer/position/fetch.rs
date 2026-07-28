//! Fenced Fetch terminal transitions for one assigned partition.

use super::AssignedPartitionState;
use crate::{
    Moment,
    consumer::{
        AssignedConsumerEffect, AssignedConsumerMachineError, FetchFailure, FetchFence,
        FetchRecords, NextFetchOffset,
    },
};

impl AssignedPartitionState {
    pub(in crate::consumer) fn fetch_advanced(
        &mut self,
        supplied: FetchFence,
        records: FetchRecords,
        next_offset: NextFetchOffset,
        now: Moment,
        throttle_ticks: u64,
    ) -> Result<Vec<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        self.ensure_position_fence(supplied.position())?;
        if self.paused {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        }
        self.position.advance(
            supplied,
            records,
            next_offset,
            now,
            throttle_ticks,
            self.partition,
        )
    }

    pub(in crate::consumer) fn fetch_failed(
        &mut self,
        supplied: FetchFence,
        failure: FetchFailure,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.ensure_position_fence(supplied.position())?;
        if self.paused {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        }
        self.position.fetch_failed(supplied, failure)
    }

    pub(in crate::consumer) fn fetch_throttle_elapsed(
        &mut self,
        supplied: FetchFence,
        now: Moment,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.ensure_position_fence(supplied.position())?;
        if self.paused {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        }
        self.position.fetch_throttle_elapsed(supplied, now)
    }
}
