//! Producer shutdown ordering and notifier handoff scenarios.

use kafka_client_core::{Deadline, Moment};

use crate::{
    ProducerDeliveryError, ProducerDeliveryFailureKind, completion::CompletionRegistryError,
};

use super::{
    admission_test::{admit, record},
    host_limits_test::{start, valid_limits},
};

#[test]
fn notifier_handoff_requires_terminal_settlement() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );

    assert!(host.stop_notifier().is_err());
    host.execution_unavailable(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("shutdown settlement should succeed: {error}"));
    let join = host
        .stop_notifier()
        .unwrap_or_else(|error| panic!("settled notifier should stop: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier should join off-host: {error}"));

    let Err(ProducerDeliveryError::Failed(failure)) = admitted.into_delivery_observer().wait()
    else {
        panic!("shutdown should publish a terminal failure")
    };
    assert_eq!(
        failure.kind(),
        ProducerDeliveryFailureKind::ExecutionUnavailable
    );
}

#[test]
fn recovery_retains_notifier_owner_when_terminal_settlement_is_damaged() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );

    let recovery = host.recover_notifier();

    assert_eq!(
        recovery.error,
        Some(CompletionRegistryError::UnsettledCompletion)
    );
    drop(admitted);
    recovery
        .notifier
        .unwrap_or_else(|| panic!("recovery must retain notifier join ownership"))
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("retained notifier should remain joinable: {error}"));
}
