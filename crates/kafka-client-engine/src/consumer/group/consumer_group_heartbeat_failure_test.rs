//! Driver and completion failures normalize before deterministic membership policy.

use kafka_client_core::ConsumerGroupHeartbeatFailure;

use crate::driver::{
    ConsumerGroupHeartbeatCompletionError, ConsumerGroupHeartbeatDriverFailureKind,
};

use super::consumer_group_heartbeat_failure::{completion_failure, driver_failure};

#[test]
fn compatibility_deadline_transport_and_malformed_terminals_stay_distinct() {
    assert_eq!(
        driver_failure(ConsumerGroupHeartbeatDriverFailureKind::Compatibility),
        ConsumerGroupHeartbeatFailure::Compatibility
    );
    assert_eq!(
        driver_failure(ConsumerGroupHeartbeatDriverFailureKind::DeadlineElapsed),
        ConsumerGroupHeartbeatFailure::DeadlineElapsed
    );
    assert_eq!(
        driver_failure(ConsumerGroupHeartbeatDriverFailureKind::Transport),
        ConsumerGroupHeartbeatFailure::CoordinatorUnavailable
    );
    assert_eq!(
        driver_failure(ConsumerGroupHeartbeatDriverFailureKind::InvalidResponse),
        ConsumerGroupHeartbeatFailure::InvalidResponse
    );
    assert_eq!(
        completion_failure(ConsumerGroupHeartbeatCompletionError::Consumed),
        ConsumerGroupHeartbeatFailure::InvalidResponse
    );
}
