//! Exhaustive dispatch from normalized KIP-848 facts to transition owners.

use super::{
    ConsumerGroupHeartbeatApplyError, ConsumerGroupHeartbeatInput, ConsumerGroupHeartbeatMachine,
    ConsumerGroupHeartbeatTransition,
};

impl ConsumerGroupHeartbeatMachine {
    /// Applies one normalized fact without I/O, ambient time, or callbacks.
    pub fn apply(
        &mut self,
        input: ConsumerGroupHeartbeatInput,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatApplyError> {
        let result = match input {
            ConsumerGroupHeartbeatInput::Begin { now, deadline } => self.begin(now, deadline),
            ConsumerGroupHeartbeatInput::HeartbeatDue { schedule, now } => {
                self.heartbeat_due(schedule, now)
            }
            ConsumerGroupHeartbeatInput::HeartbeatSucceeded {
                attempt,
                now,
                member_id,
                member_epoch,
                heartbeat_interval_ticks,
                throttle_ticks,
                assignment,
            } => self.heartbeat_succeeded(
                attempt,
                now,
                member_id,
                member_epoch,
                heartbeat_interval_ticks,
                throttle_ticks,
                assignment,
            ),
            ConsumerGroupHeartbeatInput::AssignmentRetired {
                now,
                member_id,
                member_epoch,
                assignment_generation,
            } => self.assignment_retired(now, member_id, member_epoch, assignment_generation),
            ConsumerGroupHeartbeatInput::HeartbeatFailed { attempt, failure } => {
                self.heartbeat_failed(attempt, failure)
            }
            ConsumerGroupHeartbeatInput::RetryHeartbeat {
                attempt,
                now,
                failure,
            } => self.retry_heartbeat(attempt, now, failure),
            ConsumerGroupHeartbeatInput::RediscoveryFailed { schedule, failure } => {
                self.rediscovery_failed(schedule, failure)
            }
            ConsumerGroupHeartbeatInput::RetryCoordinatorLoad {
                attempt,
                now,
                failure,
            } => self.retry_coordinator_load(attempt, now, failure),
            ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue { schedule, now } => {
                self.coordinator_load_retry_due(schedule, now)
            }
            ConsumerGroupHeartbeatInput::RecoverFencedMembership {
                attempt,
                now,
                failure,
            } => self.recover_fenced_membership(attempt, now, failure),
            ConsumerGroupHeartbeatInput::BeginLeave { now, deadline } => {
                self.begin_leave(now, deadline)
            }
            ConsumerGroupHeartbeatInput::LeaveSucceeded { attempt } => {
                self.leave_succeeded(attempt)
            }
            ConsumerGroupHeartbeatInput::LeaveFailed { attempt, failure } => {
                self.leave_failed(attempt, failure)
            }
            ConsumerGroupHeartbeatInput::Close => self.close(),
        };
        result.map_err(ConsumerGroupHeartbeatApplyError::new)
    }
}
