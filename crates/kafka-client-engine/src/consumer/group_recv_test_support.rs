//! Hosted group fixture for runtime-neutral receive observation tests.

use std::sync::Arc;

use kafka_client_core::GroupId;

use super::{
    GroupConsumerHandle, GroupConsumerRegistration,
    group::{
        GroupConsumerShardOwner, GroupConsumerShardWake, GroupConsumerShardWakeError,
        install_group_session_for_public_test, install_ready_group_delivery_for_public_test,
        started_group_registry_for_public_test,
    },
};
use crate::clock::MonotonicClock;

pub(super) struct GroupRecvFixture {
    pub(super) owner: GroupConsumerShardOwner,
    pub(super) handle: GroupConsumerHandle,
    pub(super) group_id: GroupId,
}

impl GroupRecvFixture {
    pub(super) fn start() -> Self {
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
        let group_id = handle.group_id_for_test();
        {
            let mut registry = owner.lock_registry_for_test();
            install_group_session_for_public_test(&mut registry, group_id);
        }
        Self {
            owner,
            handle,
            group_id,
        }
    }

    pub(super) fn install_ready(&self, first_offset: i64) {
        let mut registry = self.owner.lock_registry_for_test();
        install_ready_group_delivery_for_public_test(&mut registry, self.group_id, first_offset);
    }

    pub(super) fn finish(self) {
        let Self {
            mut owner,
            handle,
            group_id: _group_id,
        } = self;
        drop(handle);
        let mut registry = owner.terminal_registry();
        registry
            .recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("registry recovery: {error}"));
        let commit_join = registry
            .finish_shutdown()
            .unwrap_or_else(|error| panic!("registry finish: {error}"));
        drop(registry);
        let recv_join = owner
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
