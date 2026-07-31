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
            ConsumerGroupHeartbeatInput::HeartbeatFailed { attempt, failure } => {
                self.heartbeat_failed(attempt, failure)
            }
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
