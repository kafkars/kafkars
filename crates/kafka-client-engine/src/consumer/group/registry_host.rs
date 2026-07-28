//! Bounded engine-host scheduling and notifier inspection for the global commit lane.

use std::thread::ThreadId;

use kafka_client_core::{Deadline, Moment};

use crate::{completion::NotifierJoin, driver::DriverOwner};

use super::{
    classic_group_position::GroupConsumerPositionTurn, offset_commit::GroupOffsetCommitTurn,
    registry::GroupConsumerRegistry, registry_fetch::GroupConsumerFetchTurn,
    registry_host_error::GroupConsumerHostError, registry_membership::GroupConsumerMembershipTurn,
    registry_processing::GroupConsumerProcessingTurn,
};

pub(crate) struct GroupConsumerRegistryTurn {
    pub(crate) progressed: bool,
    pub(crate) blocked_work: bool,
}

impl GroupConsumerRegistry {
    pub(crate) fn turn(
        &mut self,
        now: Moment,
        clock: &crate::clock::MonotonicClock,
        driver: &DriverOwner,
    ) -> Result<GroupConsumerRegistryTurn, GroupConsumerHostError> {
        let offset_commit = self
            .offset_commits
            .turn(now, driver)
            .map_err(GroupConsumerHostError::from)?;
        let fault_count = self.entry_fault_count();
        let processing = match self.turn_processing(now) {
            Ok(turn) => turn,
            Err(_error) if self.entry_fault_count() > fault_count => {
                GroupConsumerProcessingTurn::Progress
            }
            Err(error) => return Err(GroupConsumerHostError::processing(error)),
        };
        if processing == GroupConsumerProcessingTurn::Progress {
            return Ok(GroupConsumerRegistryTurn {
                progressed: true,
                blocked_work: false,
            });
        }
        let fault_count = self.entry_fault_count();
        let membership = match self.turn_membership(now, clock, driver) {
            Ok(turn) => turn,
            Err(_error) if self.entry_fault_count() > fault_count => {
                GroupConsumerMembershipTurn::Progress
            }
            Err(error) => return Err(GroupConsumerHostError::membership(error)),
        };
        let fault_count = self.entry_fault_count();
        let position = match self.turn_position(now, driver) {
            Ok(turn) => turn,
            Err(_error) if self.entry_fault_count() > fault_count => {
                GroupConsumerPositionTurn::Progress
            }
            Err(error) => return Err(GroupConsumerHostError::membership(error)),
        };
        let fetch = self
            .turn_fetch(clock, driver)
            .map_err(GroupConsumerHostError::fetch)?;
        Ok(GroupConsumerRegistryTurn {
            progressed: membership == GroupConsumerMembershipTurn::Progress
                || position == GroupConsumerPositionTurn::Progress
                || fetch == GroupConsumerFetchTurn::Progress
                || processing == GroupConsumerProcessingTurn::Progress
                || offset_commit == GroupOffsetCommitTurn::Progress,
            blocked_work: membership == GroupConsumerMembershipTurn::Blocked
                || position == GroupConsumerPositionTurn::Blocked
                || fetch == GroupConsumerFetchTurn::Blocked,
        })
    }

    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        match (
            self.membership_next_deadline(),
            min_deadline(
                self.position_next_deadline(),
                min_deadline(
                    self.fetch_next_deadline(),
                    min_deadline(
                        self.processing_next_deadline(),
                        self.offset_commits.next_deadline(),
                    ),
                ),
            ),
        ) {
            (Some(membership), Some(offset)) => Some(membership.min(offset)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        }
    }

    pub(crate) fn unsettled(&self) -> usize {
        self.membership_unsettled()
            .saturating_add(self.position_unsettled())
            .saturating_add(self.fetch_unsettled())
            .saturating_add(self.processing_unsettled())
            .saturating_add(self.offset_commits.unsettled())
    }

    pub(crate) fn notifier_thread_id(&self) -> Option<ThreadId> {
        self.offset_commits.notifier_thread_id()
    }

    pub(crate) fn take_notifier(&mut self) -> Option<NotifierJoin> {
        self.offset_commits.take_notifier()
    }
}

fn min_deadline(first: Option<Deadline>, second: Option<Deadline>) -> Option<Deadline> {
    match (first, second) {
        (Some(first), Some(second)) => Some(first.min(second)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}
