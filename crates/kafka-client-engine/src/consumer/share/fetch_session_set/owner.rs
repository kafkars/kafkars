//! Assignment-fenced ownership and fair execution of broker-local share sessions.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, GroupId, MemberId, ShareAcquisitionPolicyError, ShareFetchSessionEpoch,
    ShareFetchSessionFence, ShareGroupMemberEpoch,
};

use crate::{clock::DeadlineCapture, config::ValidatedShareConsumerFetchConfig};

use super::super::{
    fetch_routing::ShareFetchRoutedAssignment,
    fetch_session::{ShareFetchSessionOwner, ShareFetchSessionOwnerError},
    fetch_session_execution::ShareFetchExecutionError,
};

use super::config::compile_session_config;

/// Complete broker-session set for one membership assignment generation.
#[must_use = "share fetch sessions must remain hosted until released"]
pub(in crate::consumer::share) struct ShareFetchSessionSet {
    pub(super) generation: AssignmentGeneration,
    pub(super) sessions: Vec<ShareFetchSessionOwner>,
    pub(super) delivery_cursor: usize,
}

/// Stable membership identity shared by one assignment's broker sessions.
pub(in crate::consumer::share) struct ShareFetchSessionIdentity {
    group_id: GroupId,
    member_id: MemberId,
    member_epoch: ShareGroupMemberEpoch,
    group: Arc<str>,
    member: Arc<str>,
}

impl ShareFetchSessionIdentity {
    pub(in crate::consumer::share) const fn new(
        group_id: GroupId,
        member_id: MemberId,
        member_epoch: ShareGroupMemberEpoch,
        group: Arc<str>,
        member: Arc<str>,
    ) -> Self {
        Self {
            group_id,
            member_id,
            member_epoch,
            group,
            member,
        }
    }
}

impl ShareFetchSessionSet {
    pub(in crate::consumer::share) fn try_open(
        routed: ShareFetchRoutedAssignment,
        identity: &ShareFetchSessionIdentity,
        config: ValidatedShareConsumerFetchConfig,
        capture: DeadlineCapture,
    ) -> Result<Self, ShareFetchSessionSetOpenError> {
        let generation = routed.generation();
        let plans = routed.into_plans();
        if plans.is_empty() {
            return Err(ShareFetchSessionSetOpenError::Empty);
        }
        let mut sessions = Vec::new();
        sessions
            .try_reserve_exact(plans.len())
            .map_err(|_error| ShareFetchSessionSetOpenError::Allocation)?;
        let session_config = compile_session_config(config)?;
        for plan in plans {
            let fence = ShareFetchSessionFence::new(
                plan.broker_id(),
                identity.group_id,
                identity.member_id,
                identity.member_epoch,
                ShareFetchSessionEpoch::initial(),
            );
            let owner = ShareFetchSessionOwner::try_open(
                plan,
                fence,
                session_config
                    .with_identity(Arc::clone(&identity.group), Arc::clone(&identity.member)),
                capture,
            )
            .map_err(ShareFetchSessionSetOpenError::Session);
            let owner = match owner {
                Ok(owner) => owner,
                Err(error) => {
                    release_unsubmitted(sessions)?;
                    return Err(error);
                }
            };
            sessions.push(owner);
        }
        Ok(Self {
            generation,
            sessions,
            delivery_cursor: 0,
        })
    }

    pub(in crate::consumer::share) const fn generation(&self) -> AssignmentGeneration {
        self.generation
    }

    pub(in crate::consumer::share) fn len(&self) -> usize {
        self.sessions.len()
    }

    pub(in crate::consumer::share) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.sessions
            .iter()
            .filter_map(ShareFetchSessionOwner::next_deadline)
            .min()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareFetchSessionSetTurn {
    Idle,
    Progress,
    Blocked,
    NeedsPreparation(usize),
    Released,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareFetchSessionSetOpenError {
    Empty,
    Allocation,
    Policy(ShareAcquisitionPolicyError),
    Session(ShareFetchSessionOwnerError),
    Rollback(ShareFetchExecutionError),
}

pub(super) fn release_unsubmitted(
    sessions: Vec<ShareFetchSessionOwner>,
) -> Result<(), ShareFetchExecutionError> {
    for session in sessions {
        session.release_unsubmitted()?;
    }
    Ok(())
}

impl From<ShareFetchExecutionError> for ShareFetchSessionSetOpenError {
    fn from(error: ShareFetchExecutionError) -> Self {
        Self::Rollback(error)
    }
}
