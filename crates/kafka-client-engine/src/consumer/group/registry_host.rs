//! Bounded engine-host scheduling and notifier inspection for the global commit lane.

use std::thread::ThreadId;

use kafka_client_core::{Deadline, Moment};

use crate::{completion::NotifierJoin, driver::DriverOwner};

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    offset_commit::{GroupOffsetCommitHostError, GroupOffsetCommitTurn},
    registry::GroupConsumerRegistry,
    registry_membership::GroupConsumerMembershipTurn,
};

/// Concrete private group-host failure without widening offset-owner internals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GroupConsumerHostError {
    kind: GroupConsumerHostErrorKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupConsumerHostErrorKind {
    OffsetCommit(GroupOffsetCommitHostError),
    Membership(ClassicGroupExecutionError),
    MembershipUnsettled(usize),
}

impl GroupConsumerHostError {
    pub(super) const fn membership(error: ClassicGroupExecutionError) -> Self {
        Self {
            kind: GroupConsumerHostErrorKind::Membership(error),
        }
    }

    pub(super) const fn membership_unsettled(count: usize) -> Self {
        Self {
            kind: GroupConsumerHostErrorKind::MembershipUnsettled(count),
        }
    }
}

impl core::fmt::Display for GroupConsumerHostError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match &self.kind {
            GroupConsumerHostErrorKind::OffsetCommit(error) => error.fmt(formatter),
            GroupConsumerHostErrorKind::Membership(error) => {
                write!(formatter, "classic membership execution failed: {error:?}")
            }
            GroupConsumerHostErrorKind::MembershipUnsettled(count) => {
                write!(
                    formatter,
                    "{count} classic membership obligations remain unsettled"
                )
            }
        }
    }
}

impl std::error::Error for GroupConsumerHostError {}

impl From<GroupOffsetCommitHostError> for GroupConsumerHostError {
    fn from(error: GroupOffsetCommitHostError) -> Self {
        Self {
            kind: GroupConsumerHostErrorKind::OffsetCommit(error),
        }
    }
}

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
        let membership = self
            .turn_membership(now, clock, driver)
            .map_err(GroupConsumerHostError::membership)?;
        Ok(GroupConsumerRegistryTurn {
            progressed: membership == GroupConsumerMembershipTurn::Progress
                || offset_commit == GroupOffsetCommitTurn::Progress,
            blocked_work: membership == GroupConsumerMembershipTurn::Blocked,
        })
    }

    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        match (
            self.membership_next_deadline(),
            self.offset_commits.next_deadline(),
        ) {
            (Some(membership), Some(offset)) => Some(membership.min(offset)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        }
    }

    pub(crate) fn unsettled(&self) -> usize {
        self.membership_unsettled() + self.offset_commits.unsettled()
    }

    pub(crate) fn notifier_thread_id(&self) -> Option<ThreadId> {
        self.offset_commits.notifier_thread_id()
    }

    pub(crate) fn take_notifier(&mut self) -> Option<NotifierJoin> {
        self.offset_commits.take_notifier()
    }
}
