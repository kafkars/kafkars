//! Lossless explicit-close admission and detached-execution evidence.

use std::sync::Arc;

use super::{GroupConsumerCloseAdmissionErrorKind, admission::admission_error_kind};
use crate::{
    clock::MonotonicClock,
    consumer::{
        GroupConsumerHandle, GroupConsumerRegistration,
        group::{
            GroupConsumerClosePortError, GroupConsumerShardLockError, GroupConsumerShardOwner,
            GroupConsumerShardWake, GroupConsumerShardWakeError, drive_group_close_for_public_test,
            started_group_registry_for_public_test,
        },
    },
};

#[test]
fn registry_contention_remains_retryable_and_distinct_from_host_loss() {
    assert_eq!(
        admission_error_kind(GroupConsumerClosePortError::Lock(
            GroupConsumerShardLockError::Contended
        )),
        GroupConsumerCloseAdmissionErrorKind::Contended
    );
    assert_ne!(
        GroupConsumerCloseAdmissionErrorKind::Contended,
        GroupConsumerCloseAdmissionErrorKind::HostUnavailable
    );
}

#[test]
fn contended_admission_returns_the_exact_retryable_handle() {
    let mut fixture = CloseFixture::start();
    let handle = fixture.take_handle();
    let group_id = handle.group_id_for_test();
    let registry = fixture.owner.lock_registry_for_test();

    let error = handle
        .try_close()
        .err()
        .unwrap_or_else(|| panic!("contended close must reject"));

    assert_eq!(
        error.kind(),
        GroupConsumerCloseAdmissionErrorKind::Contended
    );
    let handle = error.into_handle();
    assert_eq!(handle.group_id_for_test(), group_id);
    fixture.handle = Some(handle);
    drop(registry);
    fixture.finish();
}

#[test]
fn dropping_observation_does_not_cancel_the_accepted_close() {
    let mut fixture = CloseFixture::start();
    let close = fixture
        .take_handle()
        .try_close()
        .unwrap_or_else(|error| panic!("close admission: {error}"));
    drop(close);

    let mut registry = fixture.owner.lock_registry_for_test();
    assert!(drive_group_close_for_public_test(&mut registry));
    drop(registry);
    fixture.finish();
}

struct CloseFixture {
    owner: GroupConsumerShardOwner,
    handle: Option<GroupConsumerHandle>,
}

impl CloseFixture {
    fn start() -> Self {
        let registry = started_group_registry_for_public_test();
        let (owner, port) = GroupConsumerShardOwner::new(
            registry,
            Arc::new(MonotonicClock::new()),
            Arc::new(NoopWake),
        );
        let lifetime: Arc<dyn Send + Sync> = Arc::new(());
        let handle = GroupConsumerHandle::try_register(
            port,
            lifetime,
            GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")]),
        )
        .unwrap_or_else(|error| panic!("group registration: {error}"));
        Self {
            owner,
            handle: Some(handle),
        }
    }

    fn take_handle(&mut self) -> GroupConsumerHandle {
        self.handle
            .take()
            .unwrap_or_else(|| panic!("fixture handle"))
    }

    fn finish(mut self) {
        drop(self.handle.take());
        let mut registry = self.owner.terminal_registry();
        registry
            .recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("registry recovery: {error}"));
        let commit_join = registry
            .finish_shutdown()
            .unwrap_or_else(|error| panic!("registry finish: {error}"));
        drop(registry);
        let recv_join = self
            .owner
            .stop_recv_notifier()
            .unwrap_or_else(|| panic!("group receive notifier owner"));
        commit_join
            .join_off_notifier()
            .unwrap_or_else(|error| panic!("commit notifier join: {error}"));
        recv_join
            .join_off_notifier()
            .unwrap_or_else(|error| panic!("receive notifier join: {error}"));
    }
}

struct NoopWake;

impl GroupConsumerShardWake for NoopWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        Ok(())
    }
}
