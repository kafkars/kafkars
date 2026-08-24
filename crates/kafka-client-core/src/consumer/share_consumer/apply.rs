//! Exhaustive dispatch from normalized facts to share-membership transitions.

use super::{
    ShareGroupHeartbeatApplyError, ShareGroupHeartbeatInput, ShareGroupHeartbeatMachine,
    ShareGroupHeartbeatTransition,
};

impl ShareGroupHeartbeatMachine {
    /// Applies one normalized fact without I/O, ambient time, or callbacks.
    pub fn apply(
        &mut self,
        input: ShareGroupHeartbeatInput,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatApplyError> {
        let result = match input {
            ShareGroupHeartbeatInput::Begin { now, deadline } => self.begin(now, deadline),
            ShareGroupHeartbeatInput::HeartbeatDue { schedule, now } => {
                self.heartbeat_due(schedule, now)
            }
            ShareGroupHeartbeatInput::HeartbeatSucceeded {
                attempt,
                now,
                member_epoch,
                heartbeat_interval_ticks,
                throttle_ticks,
                assignment,
            } => self.heartbeat_succeeded(
                attempt,
                now,
                member_epoch,
                heartbeat_interval_ticks,
                throttle_ticks,
                assignment,
            ),
            ShareGroupHeartbeatInput::HeartbeatFailed { attempt, failure } => {
                self.heartbeat_failed(attempt, failure)
            }
            ShareGroupHeartbeatInput::RetryHeartbeat {
                attempt,
                now,
                failure,
            } => self.retry_heartbeat(attempt, now, failure),
            ShareGroupHeartbeatInput::RetryCoordinatorLoad {
                attempt,
                now,
                failure,
            } => self.retry_coordinator_load(attempt, now, failure),
            ShareGroupHeartbeatInput::RetryDue { schedule, now } => self.retry_due(schedule, now),
            ShareGroupHeartbeatInput::RediscoveryFailed { schedule, failure } => {
                self.rediscovery_failed(schedule, failure)
            }
            ShareGroupHeartbeatInput::RecoverFencedMembership {
                attempt,
                now,
                failure,
            } => self.recover_fenced_membership(attempt, now, failure),
            ShareGroupHeartbeatInput::BeginLeave { now, deadline } => {
                self.begin_leave(now, deadline)
            }
            ShareGroupHeartbeatInput::ReplaceHeartbeatWithLeave {
                attempt,
                now,
                deadline,
            } => self.replace_heartbeat_with_leave(attempt, now, deadline),
            ShareGroupHeartbeatInput::LeaveSucceeded { attempt } => self.leave_succeeded(attempt),
            ShareGroupHeartbeatInput::LeaveFailed { attempt, failure } => {
                self.leave_failed(attempt, failure)
            }
            ShareGroupHeartbeatInput::Close => self.close(),
        };
        result.map_err(ShareGroupHeartbeatApplyError::new)
    }
}
