//! Bounded Join call registry scenarios.

use kafka_driver::CompletionError;
use kafka_wire::JoinGroupResponse;

use super::{
    join_group_calls::{JoinGroupCallReservationError, TrackedJoinGroupCalls},
    join_group_settlement::JoinGroupPoll,
    join_group_terminal_test::{deadline, key, key_with_deadline},
};

#[test]
fn retained_terminal_consumes_the_exact_bounded_slot() {
    let mut calls = TrackedJoinGroupCalls::new(1);
    let first_key = key(1);
    assert!(calls.try_reserve_join_group(first_key, "group-a").is_ok());

    calls.install_terminal_for_test(first_key, Some(3), Ok(JoinGroupResponse::default()));

    assert_eq!(calls.retained_join_group_count(), 1);
    assert_eq!(
        calls.try_reserve_join_group(first_key, "group-a").err(),
        Some(JoinGroupCallReservationError::Duplicate { key: first_key })
    );
    assert_eq!(
        calls.try_reserve_join_group(key(2), "group-b").err(),
        Some(JoinGroupCallReservationError::Capacity { limit: 1 })
    );
    assert_eq!(
        calls.poll_join_group(),
        Ok(JoinGroupPoll::TerminalReady { key: first_key })
    );
}

#[test]
fn equal_cycle_and_deadline_for_another_group_is_not_a_duplicate() {
    let deadline = deadline();
    let first = key_with_deadline(1, deadline);
    let second = key_with_deadline(2, deadline);
    let mut calls = TrackedJoinGroupCalls::new(2);
    calls.install_terminal_for_test(first, Some(3), Ok(JoinGroupResponse::default()));

    assert!(calls.try_reserve_join_group(second, "group-b").is_ok());
}

#[test]
fn same_group_and_cycle_with_a_different_deadline_is_a_duplicate() {
    let first_deadline = deadline();
    let second_deadline = crate::clock::OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(first_deadline.core().tick() + 1),
        first_deadline.transport(),
    );
    let first = key_with_deadline(1, first_deadline);
    let second = key_with_deadline(1, second_deadline);
    let mut calls = TrackedJoinGroupCalls::new(2);
    calls.install_terminal_for_test(first, Some(3), Ok(JoinGroupResponse::default()));

    assert_ne!(first.deadline(), second.deadline());
    assert_eq!(
        calls.try_reserve_join_group(second, "group-a").err(),
        Some(JoinGroupCallReservationError::Duplicate { key: second })
    );
}

#[test]
fn completion_fault_remains_retained_and_observable() {
    let mut calls = TrackedJoinGroupCalls::new(1);
    let key = key(1);
    calls.install_completion_failure_for_test(key, CompletionError::Consumed);

    let observation = match calls.poll_join_group() {
        Ok(poll) => panic!("completion corruption returned {poll:?}"),
        Err(observation) => observation,
    };

    assert_eq!(observation.key(), key);
    assert_eq!(observation.source(), CompletionError::Consumed);
    assert_eq!(calls.retained_join_group_count(), 1);
}
