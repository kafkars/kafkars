//! Producer-host execution-stop and emergency fallback scenarios.

use kafka_client_core::{BatchId, ByteCount, Deadline, Moment, PayloadId};

use crate::{
    ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    producer::{
        admission_test::{admit, record},
        host_limits_test::{start, valid_limits},
    },
};

#[test]
fn deterministic_execution_stop_settles_pre_driver_work_not_sent() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );

    host.execution_unavailable(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("execution stop should settle: {error}"));

    assert_failure(
        admitted.into_delivery_observer().wait(),
        ProducerDeliveryStatus::NotSent,
    );
    assert_eq!(host.unsettled_completions(), 0);
}

#[test]
fn damaged_interpretation_still_settles_observers_conservatively() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    let batch_id = BatchId::from_raw(1);
    host.store
        .release_batch(batch_id)
        .unwrap_or_else(|error| panic!("test corruption should release batch: {error}"));
    host.store
        .release_payload(PayloadId::from_raw(1), ByteCount::new(7))
        .unwrap_or_else(|error| panic!("test corruption should release payload: {error}"));

    host.execution_unavailable(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("emergency fallback should publish: {error}"));

    assert_failure(
        admitted.into_delivery_observer().wait(),
        ProducerDeliveryStatus::PossiblySent,
    );
    assert_eq!(host.unsettled_completions(), 0);
}

fn assert_failure(
    result: Result<crate::ProducerRecordMetadata, ProducerDeliveryError>,
    status: ProducerDeliveryStatus,
) {
    let Err(ProducerDeliveryError::Failed(failure)) = result else {
        panic!("execution stop must publish a terminal failure")
    };
    assert_eq!(
        failure.kind(),
        ProducerDeliveryFailureKind::ExecutionUnavailable
    );
    assert_eq!(failure.delivery_status(), status);
}
