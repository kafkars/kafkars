//! Assignment-fenced ownership and fair execution of broker-local share sessions.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, GroupId, MemberId, ShareAcquisitionPolicyError, ShareFetchSessionEpoch,
    ShareFetchSessionFence, ShareGroupMemberEpoch,
};

use crate::{
    clock::DeadlineCapture, config::ValidatedShareConsumerFetchConfig, driver::DriverOwner,
};

use super::{
    fetch_routing::ShareFetchRoutedAssignment,
    fetch_session::{ShareFetchSessionOwner, ShareFetchSessionOwnerError},
    fetch_session_execution::{
        ShareFetchExecutionError, ShareFetchExecutionPoll, ShareFetchSubmissionTurn,
    },
};

mod config;

use config::compile_session_config;

/// Complete broker-session set for one membership assignment generation.
#[must_use = "share fetch sessions must remain hosted until released"]
pub(super) struct ShareFetchSessionSet {
    generation: AssignmentGeneration,
    sessions: Vec<ShareFetchSessionOwner>,
}

/// Stable membership identity shared by one assignment's broker sessions.
pub(super) struct ShareFetchSessionIdentity {
    group_id: GroupId,
    member_id: MemberId,
    member_epoch: ShareGroupMemberEpoch,
    group: Arc<str>,
    member: Arc<str>,
}

impl ShareFetchSessionIdentity {
    pub(super) const fn new(
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
    pub(super) fn try_open(
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
        })
    }

    pub(super) const fn generation(&self) -> AssignmentGeneration {
        self.generation
    }

    pub(super) fn len(&self) -> usize {
        self.sessions.len()
    }

    pub(super) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.sessions
            .iter()
            .filter_map(ShareFetchSessionOwner::next_deadline)
            .min()
    }

    pub(super) fn turn(
        &mut self,
        driver: &DriverOwner,
        now: kafka_client_core::Moment,
    ) -> Result<ShareFetchSessionSetTurn, ShareFetchExecutionError> {
        let mut active = false;
        for session in &mut self.sessions {
            if !session.has_active_call() {
                continue;
            }
            active = true;
            match session.poll_execution()? {
                ShareFetchExecutionPoll::Pending => {}
                ShareFetchExecutionPoll::Terminal => {
                    return Ok(ShareFetchSessionSetTurn::Progress);
                }
            }
        }
        if let Some(session) = self
            .sessions
            .iter_mut()
            .find(|session| session.has_prepared())
        {
            return match session.submit_prepared(driver, now)? {
                ShareFetchSubmissionTurn::Submitted => Ok(ShareFetchSessionSetTurn::Progress),
                ShareFetchSubmissionTurn::Backpressured => Ok(ShareFetchSessionSetTurn::Blocked),
            };
        }
        Ok(if active {
            ShareFetchSessionSetTurn::Blocked
        } else {
            ShareFetchSessionSetTurn::Idle
        })
    }

    pub(super) fn abandon_turn(
        &mut self,
    ) -> Result<ShareFetchSessionSetTurn, ShareFetchExecutionError> {
        for session in &mut self.sessions {
            if session.discard_terminal()? {
                return Ok(ShareFetchSessionSetTurn::Progress);
            }
        }
        let mut active = false;
        for session in &mut self.sessions {
            if !session.has_active_call() {
                continue;
            }
            active = true;
            match session.poll_execution() {
                Ok(ShareFetchExecutionPoll::Pending) => {}
                Ok(ShareFetchExecutionPoll::Terminal) | Err(_) => {
                    return Ok(ShareFetchSessionSetTurn::Progress);
                }
            }
        }
        if active {
            return Ok(ShareFetchSessionSetTurn::Blocked);
        }
        Ok(ShareFetchSessionSetTurn::Released)
    }

    pub(super) fn release_unsubmitted(self) -> Result<(), ShareFetchExecutionError> {
        release_unsubmitted(self.sessions)
    }

    pub(super) fn recover_after_driver_shutdown(mut self) -> Result<(), ShareFetchExecutionError> {
        for session in &mut self.sessions {
            let _recovered = session.recover_call_after_driver_shutdown()?;
            let _discarded = session.discard_terminal()?;
        }
        release_unsubmitted(self.sessions)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchSessionSetTurn {
    Idle,
    Progress,
    Blocked,
    Released,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchSessionSetOpenError {
    Empty,
    Allocation,
    Policy(ShareAcquisitionPolicyError),
    Session(ShareFetchSessionOwnerError),
    Rollback(ShareFetchExecutionError),
}

fn release_unsubmitted(
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
