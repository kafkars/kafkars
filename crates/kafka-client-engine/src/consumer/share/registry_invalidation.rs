//! Atomic share-coordinator invalidation ownership and retry permission.

use kafka_client_core::{GroupId, Moment, ShareGroupHeartbeatFailure, ShareGroupHeartbeatPhase};

use crate::{
    clock::MonotonicClock,
    driver::{
        DriverOwner,
        share_group_heartbeat::{
            ShareCoordinatorInvalidationAdmissionFailureKind,
            ShareCoordinatorInvalidationPermission, ShareCoordinatorInvalidationPoll,
            ShareCoordinatorInvalidationTerminalFailure, ShareGroupHeartbeatRoute,
        },
    },
};

use super::{
    membership::{ShareMembershipFailureTurn, ShareMembershipRetryGate},
    registry::ShareConsumerRegistry,
    registry_heartbeat_submission::settle_local_failure,
    registry_membership::ShareMembershipHostError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareInvalidationTurn {
    Idle,
    Progress,
    Blocked,
}

impl ShareConsumerRegistry {
    pub(super) fn begin_rediscovery(
        &mut self,
        index: usize,
        now: Moment,
        clock: &MonotonicClock,
        failure: ShareGroupHeartbeatFailure,
        route: ShareGroupHeartbeatRoute,
    ) -> Result<(), ShareMembershipHostError> {
        let group_id = self
            .entries
            .get(index)
            .ok_or(ShareMembershipHostError::EffectShape)?
            .group_id();
        let (entries, invalidations) = (&mut self.entries, &mut self.invalidations);
        let permit = match invalidations.try_reserve(group_id) {
            Ok(permit) => permit,
            Err(_error) => {
                route.accept();
                settle_local_failure(
                    &mut entries[index],
                    now,
                    clock,
                    ShareGroupHeartbeatFailure::Execution,
                )?;
                return Ok(());
            }
        };
        let pending = match route.into_invalidation(group_id) {
            Ok(pending) => pending,
            Err(route) => {
                drop(permit);
                route.accept();
                settle_local_failure(
                    &mut entries[index],
                    now,
                    clock,
                    ShareGroupHeartbeatFailure::Execution,
                )?;
                return Ok(());
            }
        };
        let turn = entries[index]
            .membership
            .as_mut()
            .ok_or(ShareMembershipHostError::EffectShape)?
            .settle_failure(now, clock, failure)?;
        match turn {
            ShareMembershipFailureTurn::Rediscovery(_schedule) => {
                permit
                    .install(pending)
                    .map_err(|_pending| ShareMembershipHostError::Invalidation)?;
            }
            ShareMembershipFailureTurn::Terminal => {
                drop(permit);
                drop(pending);
            }
            ShareMembershipFailureTurn::RetryScheduled(_) | ShareMembershipFailureTurn::Rejoin => {
                drop(permit);
                drop(pending);
                return Err(ShareMembershipHostError::EffectShape);
            }
        }
        Ok(())
    }

    pub(super) fn drive_one_invalidation(
        &mut self,
        driver: &DriverOwner,
    ) -> Result<ShareInvalidationTurn, ShareMembershipHostError> {
        let poll = match self.invalidations.drive_one(driver) {
            Ok(poll) => poll,
            Err(failure)
                if failure.kind() == ShareCoordinatorInvalidationAdmissionFailureKind::Full =>
            {
                return Ok(ShareInvalidationTurn::Blocked);
            }
            Err(failure) => {
                let group_id = failure.group_id();
                if !self.invalidations.discard_queued(group_id) {
                    return Err(ShareMembershipHostError::Invalidation);
                }
                self.fail_rediscovery(group_id)?;
                return Ok(ShareInvalidationTurn::Progress);
            }
        };
        match poll {
            ShareCoordinatorInvalidationPoll::Idle => Ok(ShareInvalidationTurn::Idle),
            ShareCoordinatorInvalidationPoll::Submitted { .. } => {
                Ok(ShareInvalidationTurn::Progress)
            }
            ShareCoordinatorInvalidationPoll::Pending { .. } => Ok(ShareInvalidationTurn::Blocked),
            ShareCoordinatorInvalidationPoll::Terminal { group_id, result } => {
                self.apply_invalidation_terminal(group_id, result)?;
                Ok(ShareInvalidationTurn::Progress)
            }
        }
    }

    pub(super) fn apply_invalidation_terminal(
        &mut self,
        group_id: GroupId,
        result: Result<
            ShareCoordinatorInvalidationPermission,
            ShareCoordinatorInvalidationTerminalFailure,
        >,
    ) -> Result<(), ShareMembershipHostError> {
        let entry = self
            .entry_mut(group_id)
            .ok_or(ShareMembershipHostError::EffectShape)?;
        if rediscovery_is_terminal(entry) {
            return Ok(());
        }
        match result {
            Ok(
                ShareCoordinatorInvalidationPermission::Applied
                | ShareCoordinatorInvalidationPermission::IgnoredStale,
            ) => entry
                .membership
                .as_mut()
                .ok_or(ShareMembershipHostError::EffectShape)?
                .permit_rediscovery()
                .map_err(ShareMembershipHostError::from),
            Err(_failure) => self.fail_rediscovery(group_id),
        }
    }

    fn fail_rediscovery(&mut self, group_id: GroupId) -> Result<(), ShareMembershipHostError> {
        let entry = self
            .entry_mut(group_id)
            .ok_or(ShareMembershipHostError::EffectShape)?;
        if rediscovery_is_terminal(entry) {
            return Ok(());
        }
        entry
            .membership
            .as_mut()
            .ok_or(ShareMembershipHostError::EffectShape)?
            .fail_rediscovery(ShareGroupHeartbeatFailure::Execution)
            .map_err(ShareMembershipHostError::from)
    }
}

fn rediscovery_is_terminal(entry: &super::entry::ShareConsumerEntry) -> bool {
    entry.membership.as_ref().is_some_and(|membership| {
        membership.retry_gate() == ShareMembershipRetryGate::Open
            && matches!(
                membership.machine().phase(),
                ShareGroupHeartbeatPhase::Fatal | ShareGroupHeartbeatPhase::Closed
            )
    })
}
