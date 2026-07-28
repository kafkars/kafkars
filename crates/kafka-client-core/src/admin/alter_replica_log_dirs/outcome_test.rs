//! Lossless exact-code and scalar outcome scenarios.

use core::num::NonZeroI16;

use super::{AlterReplicaLogDirBrokerError, AlterReplicaLogDirOutcome, AlterReplicaLogDirResult};

#[test]
fn broker_failure_retains_replica_identity_and_exact_signed_code() {
    let outcome = AlterReplicaLogDirOutcome::broker_failed(
        7,
        "orders".to_owned(),
        2,
        AlterReplicaLogDirBrokerError::new(
            NonZeroI16::new(-17).unwrap_or_else(|| panic!("nonzero")),
        ),
    );

    assert_eq!(outcome.broker_id(), 7);
    assert_eq!(outcome.topic(), "orders");
    assert_eq!(outcome.partition(), 2);
    assert!(matches!(
        outcome.result(),
        AlterReplicaLogDirResult::BrokerFailed(error) if error.code() == -17
    ));
}
