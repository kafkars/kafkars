//! Lossless share-port admission, contention handoff, and wake scenarios.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use super::{
    port::ShareRegistrationPortFailureSource,
    registry::ShareConsumerRegistry,
    shard::{ShareConsumerShardLockError, ShareConsumerShardOwner},
    shard_wake::{ShareConsumerShardWake, ShareConsumerShardWakeError},
};

struct CountingWake {
    requests: AtomicUsize,
    fail: bool,
}

impl ShareConsumerShardWake for CountingWake {
    fn request_share_turn(&self) -> Result<(), ShareConsumerShardWakeError> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        if self.fail {
            Err(ShareConsumerShardWakeError::from_io(io::Error::other(
                "injected wake failure",
            )))
        } else {
            Ok(())
        }
    }
}

#[test]
fn registration_and_start_retain_post_commit_wake_failures() {
    let (owner, port, wake) = setup(true);
    let accepted = port
        .try_register(
            Arc::from("workers"),
            Some(Arc::from("rack-a")),
            vec![Arc::from("jobs")],
        )
        .unwrap_or_else(|_failure| panic!("registration"));
    assert!(accepted.wake_failed());
    let capture = port
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let started = port
        .try_begin(accepted.group_id(), capture)
        .unwrap_or_else(|error| panic!("start: {error:?}"));
    assert!(started.wake_failed());
    assert_eq!(wake.requests.load(Ordering::Relaxed), 2);
    assert!(owner.terminal_registry().unsettled() > 0);
}

#[test]
fn contended_registration_returns_exact_names_and_defers_one_host_lock() {
    let (owner, port, _wake) = setup(false);
    let group = Arc::<str>::from("workers");
    let rack = Arc::<str>::from("rack-a");
    let topic = Arc::<str>::from("jobs");
    let topics = vec![Arc::clone(&topic)];
    let topics_pointer = topics.as_ptr();
    let lock = owner.lock_registry_for_test();

    let failure = port
        .try_register(Arc::clone(&group), Some(Arc::clone(&rack)), topics)
        .err()
        .unwrap_or_else(|| panic!("contention must reject"));
    assert_eq!(
        failure.source,
        ShareRegistrationPortFailureSource::Lock(ShareConsumerShardLockError::Contended)
    );
    assert!(Arc::ptr_eq(&failure.group, &group));
    assert!(
        failure
            .rack
            .as_ref()
            .is_some_and(|value| Arc::ptr_eq(value, &rack))
    );
    assert_eq!(failure.topics.as_ptr(), topics_pointer);
    assert!(Arc::ptr_eq(&failure.topics[0], &topic));
    drop(lock);

    assert_eq!(
        owner.try_registry_for_host_turn().err(),
        Some(ShareConsumerShardLockError::Contended)
    );
    drop(
        owner
            .try_registry_for_host_turn()
            .unwrap_or_else(|error| panic!("resumed host lock: {error:?}")),
    );
}

fn setup(
    fail: bool,
) -> (
    ShareConsumerShardOwner,
    super::port::ShareConsumerPort,
    Arc<CountingWake>,
) {
    let registry =
        ShareConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error}"));
    let wake = Arc::new(CountingWake {
        requests: AtomicUsize::new(0),
        fail,
    });
    let owner = ShareConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::clone(&wake),
    );
    let port = owner.admission_port();
    (owner, port, wake)
}
