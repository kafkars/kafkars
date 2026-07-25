//! Bounded engine-host scheduling and notifier inspection for the global commit lane.

use std::thread::ThreadId;

use kafka_client_core::{Deadline, Moment};

use crate::{completion::NotifierJoin, driver::DriverOwner};

use super::{
    offset_commit::{GroupOffsetCommitHostError, GroupOffsetCommitTurn},
    registry::GroupConsumerRegistry,
};

/// Concrete private group-host failure without widening offset-owner internals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GroupConsumerHostError(GroupOffsetCommitHostError);

impl core::fmt::Display for GroupConsumerHostError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for GroupConsumerHostError {}

impl From<GroupOffsetCommitHostError> for GroupConsumerHostError {
    fn from(error: GroupOffsetCommitHostError) -> Self {
        Self(error)
    }
}

impl GroupConsumerRegistry {
    pub(crate) fn turn(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<bool, GroupConsumerHostError> {
        self.offset_commits
            .turn(now, driver)
            .map_err(GroupConsumerHostError::from)
            .map(|turn| turn == GroupOffsetCommitTurn::Progress)
    }

    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        self.offset_commits.next_deadline()
    }

    pub(crate) fn unsettled(&self) -> usize {
        self.offset_commits.unsettled()
    }

    pub(crate) fn notifier_thread_id(&self) -> Option<ThreadId> {
        self.offset_commits.notifier_thread_id()
    }

    pub(crate) fn take_notifier(&mut self) -> Option<NotifierJoin> {
        self.offset_commits.take_notifier()
    }
}
