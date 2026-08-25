//! Registry and receive-boundary observation of retained group Fetch failures.

use std::{
    future::Future,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use kafka_client_core::{
    FetchFailure, GroupPositionBatch, GroupPositionFence, GroupPositionPartitionFact, Moment,
    NextFetchOffset,
};

use crate::consumer::{
    GroupConsumerFetchFailureKind, GroupConsumerHandle, GroupConsumerTryTakeBatchErrorKind,
};

use super::{
    classic_group_fetch::install_retained_fetch_failure_for_test,
    classic_group_position::test_support::completed_ready,
    registry::GroupConsumerRegistry,
    registry_delivery_error::GroupConsumerDeliveryError,
    registry_shard::GroupConsumerShardOwner,
    registry_test_support::{install_session, register, started_registry, stop_registry},
};

#[test]
fn registry_returns_one_exact_fetch_terminal_before_ordinary_delivery() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    install_retained_transport_failure(&mut registry, group_id);
    let clock = crate::clock::MonotonicClock::new();

    assert!(matches!(
        registry.take_delivery(group_id, &clock),
        Err(GroupConsumerDeliveryError::FetchTerminal(
            GroupConsumerFetchFailureKind::Transport
        ))
    ));
    assert!(matches!(registry.take_delivery(group_id, &clock), Ok(None)));

    stop_registry(&mut registry);
}

#[test]
fn dropping_recv_does_not_consume_the_retained_fetch_terminal() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    install_retained_transport_failure(&mut registry, group_id);
    let (mut owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::new(NoopWake),
    );
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = GroupConsumerHandle::from_registered_for_test(port, lifetime, group_id);

    drop(handle.recv());
    let error = handle
        .try_take_batch()
        .err()
        .unwrap_or_else(|| panic!("retained Fetch terminal"));
    assert_eq!(
        error.kind(),
        GroupConsumerTryTakeBatchErrorKind::Fetch(GroupConsumerFetchFailureKind::Transport)
    );
    assert!(matches!(handle.try_take_batch(), Ok(None)));

    drop(handle);
    let mut registry = owner.terminal_registry();
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry finish: {error}"));
    drop(registry);
    let recv_join = owner
        .stop_recv_notifier()
        .unwrap_or_else(|| panic!("group receive notifier owner"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
    recv_join
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("receive notifier join: {error}"));
}

#[test]
fn pending_recv_is_woken_by_a_new_retained_fetch_terminal() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let (mut owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::new(NoopWake),
    );
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = GroupConsumerHandle::from_registered_for_test(port, lifetime, group_id);
    let mut registry = owner.lock_registry_for_test();
    let wake = Arc::new(WakeProbe::new());
    let waker = Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    let mut recv = Box::pin(handle.recv());

    assert!(matches!(recv.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(wake.count(), 0);
    install_retained_transport_failure(&mut registry, group_id);
    owner.notify_recv_change();
    assert!(
        wake.wait_for(Duration::from_secs(2)),
        "retained Fetch terminal did not wake the armed receive"
    );
    drop(registry);
    assert!(matches!(
        recv.as_mut().poll(&mut context),
        Poll::Ready(Err(error))
            if error.kind()
                == crate::consumer::GroupConsumerRecvErrorKind::Fetch(
                    GroupConsumerFetchFailureKind::Transport,
                )
    ));
    drop(recv);
    assert!(matches!(handle.try_take_batch(), Ok(None)));

    drop(handle);
    let mut registry = owner.terminal_registry();
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry finish: {error}"));
    drop(registry);
    let recv_join = owner
        .stop_recv_notifier()
        .unwrap_or_else(|| panic!("group receive notifier owner"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
    recv_join
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("receive notifier join: {error}"));
}

fn install_retained_transport_failure(
    registry: &mut GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
) {
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group"));
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("live assignment"));
    let partition = assignment
        .partitions()
        .first()
        .copied()
        .unwrap_or_else(|| panic!("assigned partition"));
    let fence = GroupPositionFence::new(
        assignment.group_id(),
        entry
            .classic
            .machine()
            .active_cycle()
            .unwrap_or_else(|| panic!("active membership cycle")),
        assignment.member_id(),
        assignment.assignment_generation(),
    );
    entry
        .fetch
        .try_activate(
            completed_ready(
                fence,
                Moment::from_tick(41),
                GroupPositionBatch::new(
                    0,
                    vec![GroupPositionPartitionFact::committed(
                        partition,
                        NextFetchOffset::try_from_raw(17)
                            .unwrap_or_else(|| panic!("next Fetch offset")),
                    )],
                ),
            ),
            fence,
        )
        .unwrap_or_else(|_error| panic!("Fetch activation failed"));
    install_retained_fetch_failure_for_test(
        &mut entry.fetch,
        &entry.catalog,
        FetchFailure::Transport,
    );
}

struct NoopWake;

impl super::registry_wake::GroupConsumerShardWake for NoopWake {
    fn request_group_turn(&self) -> Result<(), super::registry_wake::GroupConsumerShardWakeError> {
        Ok(())
    }
}

struct WakeProbe {
    state: Mutex<usize>,
    changed: Condvar,
}

impl WakeProbe {
    const fn new() -> Self {
        Self {
            state: Mutex::new(0),
            changed: Condvar::new(),
        }
    }

    fn count(&self) -> usize {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn wait_for(&self, timeout: Duration) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *state > 0 {
            return true;
        }
        let (state, _timeout) = self
            .changed
            .wait_timeout(state, timeout)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state > 0
    }
}

impl Wake for WakeProbe {
    fn wake(self: Arc<Self>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = state.saturating_add(1);
        self.changed.notify_all();
    }
}
