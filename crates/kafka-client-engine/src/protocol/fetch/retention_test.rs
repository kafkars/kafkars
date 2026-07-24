//! Exact descriptor and visible-byte accounting for retained Fetch batches.

use core::mem::size_of;

use bytes::Bytes;

use super::{
    FetchBatch, FetchHeader, FetchOutputReservation, FetchProducerIdentity, FetchRecord,
    FetchTimestampType,
    retention::{FetchRetentionFailure, settle},
};

#[test]
fn exact_charge_counts_descriptor_capacity_and_visible_byte_spans() {
    let batches = vec![batch()];
    let expected = size_of::<FetchBatch>()
        + size_of::<FetchRecord>()
        + size_of::<FetchHeader>()
        + 3
        + 5
        + 5
        + 7;
    let charge = settle(
        FetchOutputReservation::from_acquired_capacity(expected),
        &batches,
    )
    .unwrap_or_else(|(failure, _)| panic!("exact reservation: {failure:?}"));

    assert_eq!(charge.reserved_bytes(), expected);
    assert_eq!(charge.retained_bytes(), expected);
    assert_eq!(charge.unused_bytes(), 0);
}

#[test]
fn insufficient_capacity_returns_the_same_reservation_for_release() {
    let batches = vec![batch()];
    let reservation = FetchOutputReservation::from_acquired_capacity(1);
    let Err((failure, reservation)) = settle(reservation, &batches) else {
        panic!("one byte cannot retain batch");
    };

    assert!(matches!(
        failure,
        FetchRetentionFailure::ReservationExceeded {
            actual,
            reserved: 1,
        } if actual > 1
    ));
    assert_eq!(reservation.bytes(), 1);
}

fn batch() -> FetchBatch {
    FetchBatch {
        base_offset: 4,
        last_offset: 4,
        next_offset: 5,
        partition_leader_epoch: None,
        timestamp_type: FetchTimestampType::Create,
        max_timestamp: Some(9),
        producer: Some(FetchProducerIdentity {
            producer_id: 1,
            producer_epoch: 2,
            base_sequence: 3,
        }),
        is_transactional: false,
        is_control: false,
        delete_horizon_ms: None,
        records: vec![FetchRecord {
            attributes: 0,
            offset: 4,
            timestamp: Some(9),
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"value")),
            headers: vec![FetchHeader {
                key: Bytes::from_static(b"trace"),
                value: Some(Bytes::from_static(b"payload")),
            }],
        }],
    }
}
