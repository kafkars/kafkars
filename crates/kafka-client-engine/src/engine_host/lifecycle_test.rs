//! Retained lifecycle-report state scenarios.

use super::{EngineHostControl, EngineLifecycle, EngineShutdownErrorKind};

use crate::{EngineConfig, driver::DriverOwner};

#[test]
fn closed_is_published_only_by_terminal_owner() {
    let lifecycle = EngineLifecycle::new();
    assert!(!lifecycle.is_closed());

    lifecycle.publish(None);

    assert!(lifecycle.is_closed());
}

#[test]
fn notifier_worker_cannot_wait_for_its_own_join() {
    let lifecycle = EngineLifecycle::new();
    lifecycle.install_notifier_thread(std::thread::current().id());
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("test driver should build: {error}"));
    let control = EngineHostControl::new(driver.reactor_wake());

    let error = lifecycle
        .request_and_wait(&control)
        .err()
        .unwrap_or_else(|| panic!("notification worker must not wait for its own join"));

    assert_eq!(error.kind(), EngineShutdownErrorKind::NotifierThread);
}
