//! Immutable owner of one fenced direct-Fetch throttle deadline.

use super::{AssignedConsumerMachineError, FetchFence, NextFetchOffset};
use crate::{Deadline, Moment};

#[derive(Debug)]
pub(super) struct FetchThrottle {
    fence: FetchFence,
    next_offset: NextFetchOffset,
    deadline: Deadline,
}

impl FetchThrottle {
    pub(super) const fn new(
        fence: FetchFence,
        next_offset: NextFetchOffset,
        deadline: Deadline,
    ) -> Self {
        Self {
            fence,
            next_offset,
            deadline,
        }
    }

    pub(super) const fn next_offset(&self) -> NextFetchOffset {
        self.next_offset
    }

    pub(super) const fn deadline(&self) -> Deadline {
        self.deadline
    }

    pub(super) fn ensure_elapsed(
        &self,
        supplied: FetchFence,
        now: Moment,
    ) -> Result<(), AssignedConsumerMachineError> {
        if supplied != self.fence {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        }
        if !self.deadline.is_elapsed_at(now) {
            return Err(
                AssignedConsumerMachineError::FetchThrottleDeadlineNotElapsed {
                    fence: supplied,
                    deadline: self.deadline,
                    now,
                },
            );
        }
        Ok(())
    }
}
