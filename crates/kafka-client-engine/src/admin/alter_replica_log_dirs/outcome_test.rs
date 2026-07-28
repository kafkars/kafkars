//! Stable engine outcome scalar and observer-error tests.

use core::num::NonZeroI16;

use kafka_client_core::{
    AlterReplicaLogDirBrokerError as CoreBrokerError, AlterReplicaLogDirOutcome as CoreOutcome,
    AlterReplicaLogDirsBatch as CoreBatch, AlterReplicaLogDirsTerminal as CoreTerminal,
};

use super::{
    AlterReplicaLogDirEngineResult, AlterReplicaLogDirsObserverError, AlterReplicaLogDirsOutcome,
    outcome::translate_terminal,
};

#[test]
fn exact_identity_order_throttle_and_signed_code_cross_engine_boundary() {
    let terminal = CoreTerminal::Altered(CoreBatch::new(
        19,
        vec![
            CoreOutcome::altered(7, "orders".to_owned(), 2),
            CoreOutcome::broker_failed(
                3,
                "audit".to_owned(),
                0,
                CoreBrokerError::new(NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero"))),
            ),
        ],
    ));

    let AlterReplicaLogDirsOutcome::Altered(batch) = translate_terminal(terminal) else {
        panic!("altered batch expected");
    };
    let (throttle_time_ms, outcomes) = batch.into_parts();
    assert_eq!(throttle_time_ms, 19);

    let mut outcomes = outcomes.into_iter();
    let (broker_id, topic, partition, result) = outcomes
        .next()
        .unwrap_or_else(|| panic!("first outcome"))
        .into_parts();
    assert_eq!((broker_id, topic.as_str(), partition), (7, "orders", 2));
    assert_eq!(result, AlterReplicaLogDirEngineResult::Altered);

    let (broker_id, topic, partition, result) = outcomes
        .next()
        .unwrap_or_else(|| panic!("second outcome"))
        .into_parts();
    assert_eq!((broker_id, topic.as_str(), partition), (3, "audit", 0));
    let AlterReplicaLogDirEngineResult::BrokerFailed(error) = result else {
        panic!("broker failure expected");
    };
    assert_eq!(error.code(), -32_000);
    assert!(outcomes.next().is_none());
}

#[test]
fn observer_errors_have_stable_operation_specific_diagnostics() {
    assert_eq!(
        AlterReplicaLogDirsObserverError::AlreadyObserved.to_string(),
        "Admin AlterReplicaLogDirs result was already observed"
    );
    assert_eq!(
        AlterReplicaLogDirsObserverError::Stale.to_string(),
        "Admin AlterReplicaLogDirs observer is stale"
    );
}
