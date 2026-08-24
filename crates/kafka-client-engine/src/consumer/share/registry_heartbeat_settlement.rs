//! Atomic `ShareGroupHeartbeat` terminal normalization and route disposition.

use kafka_client_core::{Moment, ShareGroupHeartbeatFailure, ShareGroupHeartbeatRequestKind};

use crate::{
    clock::MonotonicClock,
    driver::{
        ConsumerGroupHeartbeatDriverFailureKind,
        share_group_heartbeat::{
            ShareGroupHeartbeatCompletionError, ShareGroupHeartbeatResolution,
        },
    },
};

use super::{
    registry::ShareConsumerRegistry, registry_heartbeat_submission::settle_local_failure,
    registry_membership::ShareMembershipHostError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareHeartbeatSettlementTurn {
    Idle,
    Progress,
    Blocked,
}

impl ShareConsumerRegistry {
    pub(super) fn settle_one_heartbeat(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
    ) -> Result<ShareHeartbeatSettlementTurn, ShareMembershipHostError> {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.heartbeat_call.is_some())
        else {
            return Ok(ShareHeartbeatSettlementTurn::Idle);
        };
        let entry = &mut self.entries[index];
        let call = entry
            .take_heartbeat_call()
            .ok_or(ShareMembershipHostError::EffectShape)?;
        let kind = entry
            .membership
            .as_ref()
            .and_then(super::ShareMembershipInterpreter::prepared)
            .ok_or(ShareMembershipHostError::EffectShape)?
            .kind;
        let Some(terminal) = call.try_result() else {
            entry
                .install_heartbeat_call(call)
                .map_err(|_call| ShareMembershipHostError::EffectShape)?;
            return Ok(ShareHeartbeatSettlementTurn::Blocked);
        };
        drop(call);
        let outcome = match terminal {
            Ok(outcome) => outcome,
            Err(error) => {
                settle_local_failure(entry, now, clock, completion_failure(error))?;
                return Ok(ShareHeartbeatSettlementTurn::Progress);
            }
        };
        let (resolution, route) = outcome.into_resolution();
        match resolution {
            ShareGroupHeartbeatResolution::Succeeded(_success)
                if kind == ShareGroupHeartbeatRequestKind::Leave =>
            {
                route.accept();
                entry
                    .membership
                    .as_mut()
                    .ok_or(ShareMembershipHostError::EffectShape)?
                    .settle_leave_success()?;
            }
            ShareGroupHeartbeatResolution::Succeeded(success) => {
                route.accept();
                entry
                    .membership
                    .as_mut()
                    .ok_or(ShareMembershipHostError::EffectShape)?
                    .settle_success(now, success)?;
            }
            ShareGroupHeartbeatResolution::BrokerRejected { error_code, .. }
                if matches!(error_code, 15 | 16) =>
            {
                self.begin_rediscovery(
                    index,
                    now,
                    clock,
                    ShareGroupHeartbeatFailure::Broker(error_code),
                    route,
                )?;
            }
            ShareGroupHeartbeatResolution::BrokerRejected { error_code, .. } => {
                route.accept();
                settle_local_failure(
                    &mut self.entries[index],
                    now,
                    clock,
                    ShareGroupHeartbeatFailure::Broker(error_code),
                )?;
            }
            ShareGroupHeartbeatResolution::Failed(failure) => {
                if let Some(failure) =
                    rediscovery_failure(kind, failure, route.has_coordinator_token())
                {
                    self.begin_rediscovery(index, now, clock, failure, route)?;
                } else {
                    let failure = driver_failure(failure);
                    route.accept();
                    settle_local_failure(&mut self.entries[index], now, clock, failure)?;
                }
            }
        }
        Ok(ShareHeartbeatSettlementTurn::Progress)
    }
}

pub(super) fn rediscovery_failure(
    kind: ShareGroupHeartbeatRequestKind,
    failure: ConsumerGroupHeartbeatDriverFailureKind,
    has_coordinator_route: bool,
) -> Option<ShareGroupHeartbeatFailure> {
    match failure {
        ConsumerGroupHeartbeatDriverFailureKind::Transport => {
            Some(ShareGroupHeartbeatFailure::CoordinatorUnavailable)
        }
        ConsumerGroupHeartbeatDriverFailureKind::DeadlineElapsed
            if kind == ShareGroupHeartbeatRequestKind::Steady && has_coordinator_route =>
        {
            Some(ShareGroupHeartbeatFailure::CoordinatorUnavailable)
        }
        _ => None,
    }
}

const fn completion_failure(
    _failure: ShareGroupHeartbeatCompletionError,
) -> ShareGroupHeartbeatFailure {
    ShareGroupHeartbeatFailure::Execution
}

pub(super) const fn driver_failure(
    failure: ConsumerGroupHeartbeatDriverFailureKind,
) -> ShareGroupHeartbeatFailure {
    match failure {
        ConsumerGroupHeartbeatDriverFailureKind::DeadlineElapsed => {
            ShareGroupHeartbeatFailure::DeadlineElapsed
        }
        ConsumerGroupHeartbeatDriverFailureKind::Compatibility => {
            ShareGroupHeartbeatFailure::Compatibility
        }
        ConsumerGroupHeartbeatDriverFailureKind::Transport => {
            ShareGroupHeartbeatFailure::CoordinatorUnavailable
        }
        ConsumerGroupHeartbeatDriverFailureKind::InvalidResponse
        | ConsumerGroupHeartbeatDriverFailureKind::ResponseTooLarge => {
            ShareGroupHeartbeatFailure::InvalidResponse
        }
        ConsumerGroupHeartbeatDriverFailureKind::DriverRejected => {
            ShareGroupHeartbeatFailure::Execution
        }
    }
}
