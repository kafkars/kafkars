//! Bounded classic Heartbeat call registry scenarios.

use kafka_driver::CompletionError;
use kafka_wire::HeartbeatResponse;

use super::{
    heartbeat_calls::{ClassicHeartbeatCallReservationError, TrackedClassicHeartbeatCalls},
    heartbeat_settlement::ClassicHeartbeatPoll,
    heartbeat_terminal_test::{deadline, key, key_with_attempt_and_deadline, key_with_deadline},
    heartbeat_test_fixture::heartbeat_attempts,
};

#[test]
fn retained_terminal_consumes_the_exact_bounded_slot() {
    let mut calls = TrackedClassicHeartbeatCalls::new(1);
    let first_key = key(1);
    assert!(
        calls
            .try_reserve_classic_heartbeat(first_key, "group-a")
            .is_ok()
    );

    calls.install_terminal_for_test(first_key, Some(2), Ok(HeartbeatResponse::default()));

    assert_eq!(calls.retained_classic_heartbeat_count(), 1);
    assert_eq!(
        calls
            .try_reserve_classic_heartbeat(first_key, "group-a")
            .err(),
        Some(ClassicHeartbeatCallReservationError::Duplicate { key: first_key })
    );
    assert_eq!(
        calls.try_reserve_classic_heartbeat(key(2), "group-b").err(),
        Some(ClassicHeartbeatCallReservationError::Capacity { limit: 1 })
    );
    assert_eq!(
        calls.poll_classic_heartbeat(),
        Ok(ClassicHeartbeatPoll::TerminalReady { key: first_key })
    );
}

#[test]
fn equal_attempt_and_deadline_for_another_group_is_not_a_duplicate() {
    let deadline = deadline();
    let first = key_with_deadline(1, deadline);
    let second = key_with_deadline(2, deadline);
    let mut calls = TrackedClassicHeartbeatCalls::new(2);
    calls.install_terminal_for_test(first, Some(2), Ok(HeartbeatResponse::default()));

    assert!(
        calls
            .try_reserve_classic_heartbeat(second, "group-b")
            .is_ok()
    );
}

#[test]
fn same_group_and_attempt_with_a_different_deadline_is_a_duplicate() {
    let first_deadline = deadline();
    let second_deadline = crate::clock::OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(first_deadline.core().tick() + 1),
        first_deadline.transport(),
    );
    let first = key_with_deadline(1, first_deadline);
    let second = key_with_deadline(1, second_deadline);
    let mut calls = TrackedClassicHeartbeatCalls::new(2);
    calls.install_terminal_for_test(first, Some(2), Ok(HeartbeatResponse::default()));

    assert_ne!(first.deadline(), second.deadline());
    assert_eq!(
        calls.try_reserve_classic_heartbeat(second, "group-a").err(),
        Some(ClassicHeartbeatCallReservationError::Duplicate { key: second })
    );
}

#[test]
fn next_sequence_for_the_same_group_is_a_distinct_attempt() {
    let deadline = deadline();
    let (first_attempt, next_attempt) = heartbeat_attempts();
    let first = key_with_attempt_and_deadline(1, first_attempt, deadline);
    let next = key_with_attempt_and_deadline(1, next_attempt, deadline);
    let mut calls = TrackedClassicHeartbeatCalls::new(2);
    calls.install_terminal_for_test(first, Some(2), Ok(HeartbeatResponse::default()));

    assert_ne!(first.attempt(), next.attempt());
    assert!(calls.try_reserve_classic_heartbeat(next, "group-a").is_ok());
}

#[test]
fn completion_fault_remains_retained_and_observable() {
    let mut calls = TrackedClassicHeartbeatCalls::new(1);
    let key = key(1);
    calls.install_completion_failure_for_test(key, CompletionError::Consumed);

    let observation = match calls.poll_classic_heartbeat() {
        Ok(poll) => panic!("completion corruption returned {poll:?}"),
        Err(observation) => observation,
    };

    assert_eq!(observation.key(), key);
    assert_eq!(observation.source(), CompletionError::Consumed);
    assert_eq!(calls.retained_classic_heartbeat_count(), 1);
}
