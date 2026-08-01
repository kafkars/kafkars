//! Explicit terminal KIP-848 broker-error scenarios outside recoverable routing failures.

use super::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatPhase,
    test_support::{heartbeating, joining},
};

#[test]
fn unreleased_instance_id_remains_terminal_without_recovery() {
    let (mut joining, join_attempt) = joining();
    let transition = joining
        .apply(ConsumerGroupHeartbeatInput::HeartbeatFailed {
            attempt: join_attempt,
            failure: ConsumerGroupHeartbeatFailure::Broker(111),
        })
        .unwrap_or_else(|error| panic!("initial unreleased instance: {error}"));
    assert!(matches!(
        transition.into_effects().collect::<Vec<_>>().as_slice(),
        [ConsumerGroupHeartbeatEffect::Fatal { fatal }]
            if fatal.attempt() == join_attempt
                && fatal.failure() == ConsumerGroupHeartbeatFailure::Broker(111)
    ));

    let (mut steady, steady_attempt) = heartbeating();
    let transition = steady
        .apply(ConsumerGroupHeartbeatInput::HeartbeatFailed {
            attempt: steady_attempt,
            failure: ConsumerGroupHeartbeatFailure::Broker(111),
        })
        .unwrap_or_else(|error| panic!("steady unreleased instance: {error}"));
    assert!(matches!(
        transition.into_effects().collect::<Vec<_>>().as_slice(),
        [
            ConsumerGroupHeartbeatEffect::Revoke { .. },
            ConsumerGroupHeartbeatEffect::Fatal { fatal },
        ] if fatal.attempt() == steady_attempt
            && fatal.failure() == ConsumerGroupHeartbeatFailure::Broker(111)
    ));
    assert_eq!(steady.phase(), ConsumerGroupHeartbeatPhase::Fatal);
}
