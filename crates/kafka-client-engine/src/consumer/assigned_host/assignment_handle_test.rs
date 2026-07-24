//! Public assignment admission, ordering, deadline, and wake-fault scenarios.

use std::{sync::Arc, time::Duration};

use super::{
    claim::AssignedConsumerClaimSlot,
    handle::AssignedConsumerHandle,
    shard::AssignedConsumerShardOwner,
    shard_test::{FailingWake, setup},
};
use crate::{
    clock::MonotonicClock,
    consumer::{
        AssignedConsumerAssignment, AssignedConsumerStartPosition,
        AssignedConsumerTryReplaceAssignmentErrorKind,
    },
};

#[test]
fn replacement_accepts_in_caller_order_and_returns_an_opaque_epoch() {
    let (owner, port, wake) = setup();
    let mut handle = claim(port);

    let accepted = handle
        .try_replace_assignment(
            vec![assignment("orders", 1), assignment("orders", 0)],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("accept assignment: {error}"));

    assert_eq!(accepted.epoch().get(), 1);
    assert_eq!(accepted.fault(), None);
    assert_eq!(wake.count(), 1);
    let partitions = owner
        .try_with_owner(|assigned| {
            assigned
                .topics
                .partitions()
                .iter()
                .map(|entry| entry.partition().partition().get())
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|error| panic!("assigned owner: {error:?}"));
    assert_eq!(partitions, vec![1, 0]);
}

#[test]
fn duplicate_entries_reach_core_unchanged_and_are_rejected() {
    let (_owner, port, wake) = setup();
    let mut handle = claim(port);

    let error = handle
        .try_replace_assignment(
            vec![assignment("orders", 0), assignment("orders", 0)],
            Duration::from_secs(1),
        )
        .err()
        .unwrap_or_else(|| panic!("duplicate assignment must fail"));

    assert_eq!(
        error.kind(),
        AssignedConsumerTryReplaceAssignmentErrorKind::DuplicatePartition
    );
    assert_eq!(wake.count(), 0);
}

#[test]
fn deadline_capture_precedes_assignment_owner_contention() {
    let (owner, port, _wake) = setup();
    let mut handle = claim(port);
    let guard = owner.lock_for_test();

    let error = handle
        .try_replace_assignment(vec![assignment("orders", 0)], Duration::MAX)
        .err()
        .unwrap_or_else(|| panic!("unrepresentable deadline must fail"));
    drop(guard);

    assert_eq!(
        error.kind(),
        AssignedConsumerTryReplaceAssignmentErrorKind::DeadlineOverflow
    );
}

#[test]
fn accepted_assignment_retains_post_commit_wake_failure() {
    let clock = Arc::new(MonotonicClock::new());
    let wake = Arc::new(FailingWake);
    let (_owner, port) = AssignedConsumerShardOwner::new_for_test(
        clock,
        super::super::assigned_owner_test::settings(),
        super::super::assigned_owner_test::limits(2),
        wake,
    )
    .unwrap_or_else(|error| panic!("assigned shard: {error:?}"));
    let mut handle = claim(port);

    let accepted = handle
        .try_replace_assignment(vec![assignment("orders", 0)], Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("wake failure cannot revoke acceptance: {error}"));

    assert_eq!(
        accepted.fault(),
        Some(super::AssignedConsumerAcceptedFaultKind::Wake)
    );
}

fn claim(port: super::AssignedConsumerPort) -> AssignedConsumerHandle {
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    slot.claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"))
}

fn assignment(topic: &str, partition: i32) -> AssignedConsumerAssignment {
    AssignedConsumerAssignment::try_new(topic, partition, AssignedConsumerStartPosition::Beginning)
        .unwrap_or_else(|error| panic!("valid assignment input: {error}"))
}
