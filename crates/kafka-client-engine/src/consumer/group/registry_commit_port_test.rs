//! Capture ordering, exact rejection, and accepted commit observation scenarios.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kafka_client_core::{
    DeliveryStatus, GroupId, GroupOffsetCommitFailureKind, GroupOffsetCommitTerminal,
};

use super::{
    offset_commit::test_support::admission_usage,
    registry::GroupConsumerRegistry,
    registry_commit::GroupConsumerCommitFailureKind,
    registry_commit_port::GroupConsumerCommitPortFailureKind,
    registry_shard::GroupConsumerShardOwner,
    registry_test_support::{checkpoint, install_session, register},
    registry_wake::{GroupConsumerShardWake, GroupConsumerShardWakeError},
};

struct CountingWake {
    requests: AtomicUsize,
    fail: bool,
}

impl GroupConsumerShardWake for CountingWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        if self.fail {
            Err(GroupConsumerShardWakeError::from_io(io::Error::other(
                "injected commit wake failure",
            )))
        } else {
            Ok(())
        }
    }
}

#[test]
fn deadline_capture_precedes_closed_admission_and_preserves_checkpoint() {
    let (owner, port, group_id, wake) = started(false);
    let checkpoint = {
        let registry = owner
            .try_registry()
            .unwrap_or_else(|error| panic!("registry lock: {error:?}"));
        checkpoint(&registry, group_id)
    };
    let entries = checkpoint.entries().as_ptr();
    port.close_admission();
    let wake_count = wake.requests.load(Ordering::Relaxed);

    let failure = port
        .try_commit(group_id, Duration::MAX, checkpoint)
        .err()
        .unwrap_or_else(|| panic!("unrepresentable deadline must reject"));

    assert!(matches!(
        failure.kind,
        GroupConsumerCommitPortFailureKind::Clock(_)
    ));
    let checkpoint = failure.into_checkpoint();
    assert_eq!(checkpoint.entries().as_ptr(), entries);
    assert_eq!(wake.requests.load(Ordering::Relaxed), wake_count);
    let registry = owner
        .try_registry()
        .unwrap_or_else(|error| panic!("registry lock: {error:?}"));
    assert_eq!(admission_usage(&registry.offset_commits), (0, 0));
    drop(registry);
    stop(owner);
}

#[test]
fn contended_rejection_returns_exact_checkpoint_without_spending_capacity() {
    let (owner, port, group_id, wake) = started(false);
    let checkpoint = {
        let registry = owner
            .try_registry()
            .unwrap_or_else(|error| panic!("registry lock: {error:?}"));
        checkpoint(&registry, group_id)
    };
    let entries = checkpoint.entries().as_ptr();
    let registry = owner.lock_registry_for_test();
    let before = admission_usage(&registry.offset_commits);

    let failure = port
        .try_commit(group_id, Duration::from_secs(1), checkpoint)
        .err()
        .unwrap_or_else(|| panic!("contended admission must reject"));

    assert!(matches!(
        failure.kind,
        GroupConsumerCommitPortFailureKind::Lock(
            super::registry_shard::GroupConsumerShardLockError::Contended
        )
    ));
    let checkpoint = failure.into_checkpoint();
    assert_eq!(checkpoint.entries().as_ptr(), entries);
    assert_eq!(admission_usage(&registry.offset_commits), before);
    assert_eq!(wake.requests.load(Ordering::Relaxed), 0);
    drop(registry);
    stop(owner);
}

#[test]
fn registry_rejection_returns_exact_checkpoint_without_requesting_a_turn() {
    let (owner, port, group_id, wake) = started(false);
    let checkpoint = {
        let registry = owner
            .try_registry()
            .unwrap_or_else(|error| panic!("registry lock: {error:?}"));
        checkpoint(&registry, group_id)
    };
    let entries = checkpoint.entries().as_ptr();
    let unknown =
        GroupId::try_from_raw(999).unwrap_or_else(|| panic!("unknown group must be nonzero"));

    let failure = port
        .try_commit(unknown, Duration::from_secs(1), checkpoint)
        .err()
        .unwrap_or_else(|| panic!("unknown group must reject"));

    assert_eq!(
        failure.kind,
        GroupConsumerCommitPortFailureKind::Registry(GroupConsumerCommitFailureKind::UnknownGroup)
    );
    let checkpoint = failure.into_checkpoint();
    assert_eq!(checkpoint.entries().as_ptr(), entries);
    assert_eq!(wake.requests.load(Ordering::Relaxed), 0);
    let registry = owner
        .try_registry()
        .unwrap_or_else(|error| panic!("registry lock: {error:?}"));
    assert_eq!(admission_usage(&registry.offset_commits), (0, 0));
    drop(registry);
    stop(owner);
}

#[test]
fn accepted_observer_and_original_deadline_survive_advisory_wake_failure() {
    let (owner, port, group_id, wake) = started(true);
    let clock = Arc::clone(&port.clock);
    let checkpoint = {
        let registry = owner
            .try_registry()
            .unwrap_or_else(|error| panic!("registry lock: {error:?}"));
        checkpoint(&registry, group_id)
    };
    let before = clock
        .now()
        .unwrap_or_else(|error| panic!("clock before: {error}"));

    let admission = port
        .try_commit(group_id, Duration::from_secs(3), checkpoint)
        .unwrap_or_else(|failure| panic!("commit admission: {:?}", failure.kind));
    let after = clock
        .now()
        .unwrap_or_else(|error| panic!("clock after: {error}"));

    assert!(admission.wake_failed());
    assert_eq!(wake.requests.load(Ordering::Relaxed), 1);
    let registry = owner
        .try_registry()
        .unwrap_or_else(|error| panic!("registry lock: {error:?}"));
    let (retained_bytes, completion_count) = admission_usage(&registry.offset_commits);
    assert!(retained_bytes > 0);
    assert_eq!(completion_count, 1);
    let deadline = registry
        .offset_commits
        .next_deadline()
        .unwrap_or_else(|| panic!("accepted commit deadline expected"));
    assert!(deadline.tick() >= before.tick() + 3_000_000_000);
    assert!(deadline.tick() <= after.tick() + 3_000_000_000);
    drop(registry);

    let (accepted, wake_failure) = admission.into_parts();
    assert!(accepted.fault.is_none());
    assert!(wake_failure.is_some());
    let mut registry = owner.terminal_registry();
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    let terminal = accepted
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("accepted terminal: {error}"));
    let GroupOffsetCommitTerminal::Failed(failure) = terminal else {
        panic!("queued recovery must retain definitely-unsent failure");
    };
    assert_eq!(failure.kind(), GroupOffsetCommitFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
    finish(&mut registry);
    drop(registry);
    drop(owner);
}

#[test]
fn observer_drop_abandons_observation_without_cancelling_commit() {
    let (owner, port, group_id, _wake) = started(false);
    let checkpoint = {
        let registry = owner
            .try_registry()
            .unwrap_or_else(|error| panic!("registry lock: {error:?}"));
        checkpoint(&registry, group_id)
    };
    let admission = port
        .try_commit(group_id, Duration::from_secs(1), checkpoint)
        .unwrap_or_else(|failure| panic!("commit admission: {:?}", failure.kind));
    let (accepted, wake_failure) = admission.into_parts();
    assert!(accepted.fault.is_none());
    assert!(wake_failure.is_none());
    drop(accepted.observer);

    let registry = owner
        .try_registry()
        .unwrap_or_else(|error| panic!("registry lock: {error:?}"));
    let (retained_bytes, completion_count) = admission_usage(&registry.offset_commits);
    assert!(retained_bytes > 0);
    assert_eq!(completion_count, 1);
    assert_eq!(registry.offset_commits.unsettled(), 1);
    drop(registry);
    stop(owner);
}

fn started(
    fail_wake: bool,
) -> (
    GroupConsumerShardOwner,
    super::registry_port::GroupConsumerPort,
    GroupId,
    Arc<CountingWake>,
) {
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error}"));
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let wake = Arc::new(CountingWake {
        requests: AtomicUsize::new(0),
        fail: fail_wake,
    });
    let (owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::clone(&wake),
    );
    (owner, port, group_id, wake)
}

fn stop(owner: GroupConsumerShardOwner) {
    let mut registry = owner.terminal_registry();
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    finish(&mut registry);
    drop(registry);
    drop(owner);
}

fn finish(registry: &mut GroupConsumerRegistry) {
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry finish: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
