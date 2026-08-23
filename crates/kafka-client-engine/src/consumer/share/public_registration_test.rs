//! Public share registration ownership and lossless rejection scenarios.

use std::{sync::Arc, time::Duration};

use super::{
    public_registration::{
        ShareConsumerHandle, ShareConsumerRegistration, ShareConsumerStartCapture,
    },
    public_registration_error::ShareConsumerRegistrationErrorKind,
    registry::ShareConsumerRegistry,
    shard::ShareConsumerShardOwner,
    shard_wake::{ShareConsumerShardWake, ShareConsumerShardWakeError},
};

struct NoopWake;

impl ShareConsumerShardWake for NoopWake {
    fn request_share_turn(&self) -> Result<(), ShareConsumerShardWakeError> {
        Ok(())
    }
}

#[test]
fn capture_registers_and_starts_one_unique_share_handle_atomically() {
    let (owner, port) = setup();
    let capture = ShareConsumerStartCapture::capture(port.clone(), Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let handle = ShareConsumerHandle::try_register_started(
        port,
        Arc::new(()),
        capture,
        ShareConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("jobs")]),
    )
    .unwrap_or_else(|error| panic!("registration: {error:?}"));

    assert!(!handle.startup_wake_failed());
    assert!(
        owner
            .lock_registry_for_test()
            .entry(handle.group_id)
            .is_some_and(|entry| entry.start.is_some())
    );
    require_send(handle);
}

#[test]
fn invalid_registration_returns_exact_names_and_close_policy() {
    let (_owner, port) = setup();
    let capture = ShareConsumerStartCapture::capture(port.clone(), Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let group = Arc::<str>::from("workers");
    let topic = Arc::<str>::from("jobs");
    let close = Duration::from_secs(17);
    let fetch = crate::EngineShareConsumerFetchConfig::new(
        Duration::from_millis(250),
        2,
        4096,
        32,
        8,
        Duration::from_secs(9),
    );
    let error = ShareConsumerHandle::try_register_started(
        port,
        Arc::new(()),
        capture,
        ShareConsumerRegistration::new(
            Arc::clone(&group),
            vec![Arc::clone(&topic), Arc::clone(&topic)],
        )
        .with_fetch_config(fetch)
        .with_close_timeout(close),
    )
    .err()
    .unwrap_or_else(|| panic!("duplicate must reject"));

    assert_eq!(
        error.kind(),
        ShareConsumerRegistrationErrorKind::InvalidInput
    );
    let returned = error.into_registration();
    assert!(Arc::ptr_eq(&returned.group, &group));
    assert!(Arc::ptr_eq(&returned.topics[0], &topic));
    assert!(Arc::ptr_eq(&returned.topics[1], &topic));
    assert_eq!(returned.fetch_config(), fetch);
    assert_eq!(returned.close_timeout(), close);
}

fn setup() -> (ShareConsumerShardOwner, super::port::ShareConsumerPort) {
    let registry =
        ShareConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error}"));
    let owner = ShareConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::new(NoopWake),
    );
    let port = owner.admission_port();
    (owner, port)
}

fn require_send<T: Send>(_value: T) {}
