//! DescribeReplicaLogDirs error and public value translation coverage.

use crate::{DeliveryStatus, ErrorKind};

use super::{
    engine::{DeliveryStatus as EngineDeliveryStatus, FailureKind},
    result::{translate_broker_code, translate_failure_parts, translate_observer_error},
};

#[test]
fn exact_broker_code_and_delivery_are_preserved() {
    let error = translate_broker_code(-32_000);

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-32_000));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
}

#[test]
fn not_attempted_is_distinct_and_definitely_unsent() {
    let error = translate_failure_parts(FailureKind::NotAttempted, EngineDeliveryStatus::NotSent);

    assert_eq!(error.kind(), ErrorKind::State);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn stale_observer_maps_to_internal_state_loss() {
    let error = translate_observer_error(super::engine::ObserverError::Stale);

    assert_eq!(error.kind(), ErrorKind::Internal);
}
