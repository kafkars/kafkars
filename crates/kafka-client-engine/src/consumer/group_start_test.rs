//! Public membership-start acceptance and pre-core rejection contracts.

use std::{sync::Arc, time::Duration};

use super::{
    GroupConsumerHandle, GroupConsumerRegistration, GroupConsumerStartErrorKind,
    group::{
        GroupConsumerShardOwner, GroupConsumerShardWake, GroupConsumerShardWakeError,
        started_group_registry_for_public_test,
    },
};
use crate::clock::MonotonicClock;

#[test]
fn contention_is_retryable_and_the_first_core_acceptance_is_not() {
    let (owner, mut handle) = fixture(false);
    let registry = owner.lock_registry_for_test();
    let error = handle
        .try_start(Duration::from_secs(1))
        .err()
        .unwrap_or_else(|| panic!("contended membership start was accepted"));
    assert_eq!(error.kind(), GroupConsumerStartErrorKind::Contended);
    drop(registry);

    let accepted = handle
        .try_start(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("membership start: {error}"));
    assert!(!accepted.entry_faulted());
    assert!(!accepted.wake_failed());
    let error = handle
        .try_start(Duration::from_secs(1))
        .err()
        .unwrap_or_else(|| panic!("second membership start was accepted"));
    assert_eq!(error.kind(), GroupConsumerStartErrorKind::AlreadyStarted);
    finish(&owner, handle);
}

#[test]
fn post_admission_wake_failure_remains_an_accepted_membership_owner() {
    let (owner, mut handle) = fixture(true);
    let accepted = handle
        .try_start(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("membership start: {error}"));
    assert!(!accepted.entry_faulted());
    assert!(accepted.wake_failed());
    finish(&owner, handle);
}

#[test]
fn zero_timeout_is_rejected_after_capture_without_starting_membership() {
    let (owner, mut handle) = fixture(false);
    let error = handle
        .try_start(Duration::ZERO)
        .err()
        .unwrap_or_else(|| panic!("zero membership timeout was accepted"));
    assert_eq!(error.kind(), GroupConsumerStartErrorKind::InvalidTimeout);

    let accepted = handle
        .try_start(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("membership start after rejection: {error}"));
    assert!(!accepted.entry_faulted());
    assert!(!accepted.wake_failed());
    finish(&owner, handle);
}

fn fixture(wake_fails: bool) -> (GroupConsumerShardOwner, GroupConsumerHandle) {
    let registry = started_group_registry_for_public_test();
    let (owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(MonotonicClock::new()),
        Arc::new(TestWake { wake_fails }),
    );
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let handle = GroupConsumerHandle::try_register(
        port,
        lifetime,
        GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")]),
    )
    .unwrap_or_else(|error| panic!("group registration: {error}"));
    (owner, handle)
}

fn finish(owner: &GroupConsumerShardOwner, handle: GroupConsumerHandle) {
    drop(handle);
    let mut registry = owner.terminal_registry();
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry finish: {error}"));
    drop(registry);
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

struct TestWake {
    wake_fails: bool,
}

impl GroupConsumerShardWake for TestWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        if self.wake_fails {
            Err(GroupConsumerShardWakeError::from_io(std::io::Error::other(
                "test wake failure",
            )))
        } else {
            Ok(())
        }
    }
}
