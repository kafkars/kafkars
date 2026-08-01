//! Join-identity and response-boundary fences for coordinator-load retry.

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind,
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput, ConsumerGroupHeartbeatPhase,
    ConsumerGroupMemberEpoch,
    test_support::{joining, moment},
};

#[test]
fn epoch_bearing_join_attempt_cannot_cross_the_retry_seam() {
    let (mut machine, attempt) = joining();
    let corrupt = ConsumerGroupHeartbeatAttempt::new(
        attempt.sequence(),
        ConsumerGroupMemberEpoch::try_from_raw(1),
    );
    machine.in_flight = Some(corrupt);

    let error = machine
        .apply(ConsumerGroupHeartbeatInput::RetryCoordinatorLoad {
            attempt: corrupt,
            now: moment(20),
            failure: ConsumerGroupHeartbeatFailure::Broker(14),
        })
        .err()
        .unwrap_or_else(|| panic!("epoch-bearing Join retry must reject"));

    assert_eq!(
        error.kind(),
        ConsumerGroupHeartbeatErrorKind::InvariantViolation
    );
    assert_eq!(machine.in_flight(), Some(corrupt));
    assert_eq!(machine.retry_schedule(), None);
}

#[test]
fn load_response_at_the_original_deadline_terminalizes_without_arming() {
    let (mut machine, attempt) = joining();
    let effects = machine
        .apply(ConsumerGroupHeartbeatInput::RetryCoordinatorLoad {
            attempt,
            now: moment(40),
            failure: ConsumerGroupHeartbeatFailure::Broker(14),
        })
        .unwrap_or_else(|error| panic!("deadline-boundary response: {error}"))
        .into_effects()
        .collect::<Vec<_>>();

    assert!(matches!(
        effects.as_slice(),
        [ConsumerGroupHeartbeatEffect::Fatal { fatal }]
            if fatal.attempt() == attempt
                && fatal.failure() == ConsumerGroupHeartbeatFailure::DeadlineElapsed
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Fatal);
    assert_eq!(machine.retry_schedule(), None);
}
