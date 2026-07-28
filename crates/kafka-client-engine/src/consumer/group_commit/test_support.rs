//! Hosted group fixture for public checkpoint-commit boundary tests.

use std::{io, sync::Arc};

use crate::{
    clock::MonotonicClock,
    consumer::{
        GroupConsumerCheckpoint, GroupConsumerHandle, GroupConsumerRegistration,
        group::{
            GroupConsumerShardOwner, GroupConsumerShardWake, GroupConsumerShardWakeError,
            install_group_session_for_public_test, install_ready_group_delivery_for_public_test,
            started_group_registry_for_public_test,
        },
    },
};

pub(super) struct GroupCommitFixture {
    pub(super) owner: GroupConsumerShardOwner,
    pub(super) handle: GroupConsumerHandle,
}

impl GroupCommitFixture {
    pub(super) fn start(fail_wake: bool) -> Self {
        let registry = started_group_registry_for_public_test();
        let (owner, port) = GroupConsumerShardOwner::new(
            registry,
            Arc::new(MonotonicClock::new()),
            Arc::new(TestWake { fail: fail_wake }),
        );
        let lifetime: Arc<dyn Send + Sync> = Arc::new(());
        let handle = GroupConsumerHandle::try_register(
            port,
            lifetime,
            GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")]),
        )
        .unwrap_or_else(|error| panic!("group registration: {error}"));
        let group_id = handle.group_id_for_test();
        {
            let mut registry = owner.lock_registry_for_test();
            install_group_session_for_public_test(&mut registry, group_id);
            install_ready_group_delivery_for_public_test(&mut registry, group_id, 17);
        }
        Self { owner, handle }
    }

    pub(super) fn take_checkpoint(&mut self) -> GroupConsumerCheckpoint {
        self.handle
            .try_take_batch()
            .unwrap_or_else(|error| panic!("group batch observation: {error}"))
            .unwrap_or_else(|| panic!("ready group batch"))
            .into_checkpoint()
    }

    pub(super) fn finish(self) {
        drop(self.handle);
        let mut registry = self.owner.terminal_registry();
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
}

struct TestWake {
    fail: bool,
}

impl GroupConsumerShardWake for TestWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        if self.fail {
            Err(GroupConsumerShardWakeError::from_io(io::Error::other(
                "injected public commit wake failure",
            )))
        } else {
            Ok(())
        }
    }
}
