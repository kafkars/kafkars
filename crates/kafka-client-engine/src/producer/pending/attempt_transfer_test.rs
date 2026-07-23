//! Exact pending-record detach and restoration ownership scenarios.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{Deadline, PartitionIndex};

use super::{
    PendingAdmissionRegistry, PendingRecordTransferState, ProducerSendFailure,
    ProducerSendFailureKind,
};
use crate::producer::ProducerRecord;

#[test]
fn detached_record_restores_to_the_same_attempt_before_local_settlement() {
    let mut pending = PendingAdmissionRegistry::new(1, 64, 1);
    let registration = pending
        .register(
            ProducerRecord::new(
                Arc::from("orders"),
                PartitionIndex::from_raw(0),
                1,
                None,
                Some(Bytes::from_static(b"value")),
            ),
            Deadline::from_tick(40),
            Instant::now(),
        )
        .unwrap_or_else(|error| panic!("pending registration should succeed: {error:?}"));
    let send = registration.into_send();
    let mut attempt = pending
        .take_next(1)
        .unwrap_or_else(|error| panic!("promotion take should succeed: {error:?}"))
        .into_attempt()
        .unwrap_or_else(|| panic!("promotion attempt should exist"));

    let record = attempt
        .detach_record()
        .unwrap_or_else(|error| panic!("record should detach: {error:?}"));
    assert_eq!(
        attempt.transfer_state(),
        PendingRecordTransferState::Detached
    );
    attempt.restore_record(record).unwrap_or_else(|failure| {
        let (error, _record) = failure.into_parts();
        panic!("record should restore: {error:?}")
    });
    assert_eq!(
        attempt.transfer_state(),
        PendingRecordTransferState::Retained
    );

    let local = attempt
        .settle_local(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|_failure| panic!("restored attempt should settle"));
    let (admission, job) = local.into_parts();
    assert_eq!(admission.into_record().topic().as_ref(), "orders");
    job.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
}
