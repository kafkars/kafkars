//! Capture-first group port admission and advisory wake-failure scenarios.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use super::{
    classic_group_test_support,
    registry::GroupConsumerRegistry,
    registry_port::{GroupConsumerCyclePortError, GroupConsumerPortRegistrationFailureKind},
    registry_shard::GroupConsumerShardOwner,
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
                "injected wake failure",
            )))
        } else {
            Ok(())
        }
    }
}

#[test]
fn registration_preserves_exact_timing_and_one_boundary_deadline_capture() {
    let registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error}"));
    let clock = Arc::new(crate::clock::MonotonicClock::new());
    let wake = Arc::new(CountingWake {
        requests: AtomicUsize::new(0),
        fail: false,
    });
    let (owner, port) =
        GroupConsumerShardOwner::new(registry, Arc::clone(&clock), Arc::clone(&wake));
    let timing = classic_group_test_support::timing();
    let group_id = port
        .try_register(Arc::from("workers"), vec![Arc::from("orders")], timing)
        .unwrap_or_else(|failure| panic!("registration failed: {:?}", failure.kind));
    let before = clock
        .now()
        .unwrap_or_else(|error| panic!("clock observation failed: {error}"));

    let accepted = port
        .begin_cycle(group_id, Duration::from_secs(3))
        .unwrap_or_else(|error| panic!("cycle admission failed: {error:?}"));
    let after = clock
        .now()
        .unwrap_or_else(|error| panic!("clock observation failed: {error}"));

    assert_eq!(accepted.cycle().get(), 1);
    assert!(!accepted.wake_failed());
    assert_eq!(wake.requests.load(Ordering::Relaxed), 1);
    let registry = owner
        .try_registry()
        .unwrap_or_else(|error| panic!("registry lock failed: {error:?}"));
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let prepared = entry
        .execution
        .prepared_join()
        .unwrap_or_else(|| panic!("prepared Join expected"));
    assert_eq!(entry.classic.machine().timing(), timing);
    assert_eq!(prepared.timing(), timing);
    let deadline = entry
        .execution
        .next_deadline()
        .unwrap_or_else(|| panic!("accepted cycle deadline expected"));
    assert!(
        deadline.tick() >= before.tick() + 3_000_000_000,
        "deadline must be captured after the first observation"
    );
    assert!(
        deadline.tick() <= after.tick() + 3_000_000_000,
        "deadline must be captured before the second observation"
    );
    drop(registry);
    stop(owner);
}

#[test]
fn post_commit_wake_failure_does_not_reclassify_cycle_as_rejected() {
    let registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error}"));
    let clock = Arc::new(crate::clock::MonotonicClock::new());
    let wake = Arc::new(CountingWake {
        requests: AtomicUsize::new(0),
        fail: true,
    });
    let (owner, port) = GroupConsumerShardOwner::new(registry, clock, wake);
    let group_id = port
        .try_register(
            Arc::from("workers"),
            vec![Arc::from("orders")],
            classic_group_test_support::timing(),
        )
        .unwrap_or_else(|failure| panic!("registration failed: {:?}", failure.kind));

    let accepted = port
        .begin_cycle(group_id, Duration::from_secs(3))
        .unwrap_or_else(|error| panic!("accepted cycle cannot become rejection: {error:?}"));

    assert!(accepted.wake_failed());
    assert_eq!(accepted.cycle().get(), 1);
    stop(owner);
}

#[test]
fn closed_port_returns_exact_registration_names_and_rejects_deadline_work() {
    let registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error}"));
    let clock = Arc::new(crate::clock::MonotonicClock::new());
    let wake = Arc::new(CountingWake {
        requests: AtomicUsize::new(0),
        fail: false,
    });
    let (owner, port) = GroupConsumerShardOwner::new(registry, clock, wake);
    port.close_admission();
    let failure = port
        .try_register(
            Arc::from("workers"),
            vec![Arc::from("orders")],
            classic_group_test_support::timing(),
        )
        .err()
        .unwrap_or_else(|| panic!("closed registration must reject"));

    assert_eq!(
        failure.kind,
        GroupConsumerPortRegistrationFailureKind::CLOSED
    );
    assert_eq!(&*failure.group, "workers");
    assert_eq!(&*failure.local_topics[0], "orders");
    let group_id = kafka_client_core::GroupId::try_from_raw(1)
        .unwrap_or_else(|| panic!("nonzero group identity"));
    assert!(matches!(
        port.begin_cycle(group_id, Duration::from_secs(1)),
        Err(GroupConsumerCyclePortError::CLOSED)
    ));
    stop(owner);
}

fn stop(owner: GroupConsumerShardOwner) {
    let mut registry = owner.terminal_registry();
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery failed: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry stop failed: {error}"));
    drop(registry);
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join failed: {error}"));
    drop(owner);
}
