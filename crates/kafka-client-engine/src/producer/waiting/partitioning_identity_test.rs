//! Expected-topic UUID validation before waiting-record promotion.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::{Moment, PartitionIndex};

use crate::ProducerDeliveryFailureKind;

use super::super::{
    host_limits_test::start,
    record::{ProducerRecord, ProducerRecordParts, ProducerSourceOwner},
};
use super::partitioning_test::{
    assert_not_sent, deadline, partitioning_limits, topic_view, topic_view_with_uuid,
};

#[test]
fn explicit_expected_uuid_validates_before_waiting_promotion() {
    let mut host = start(partitioning_limits());
    let accepted = host
        .try_admit_waiting(
            Moment::from_tick(0),
            deadline(500),
            expected_explicit_record([7; 16]),
        )
        .unwrap_or_else(|_| panic!("expected-bound waiting admission"));
    let request = host
        .take_partitioning_request()
        .unwrap_or_else(|error| panic!("take expected UUID partitioning request: {error:?}"))
        .unwrap_or_else(|| panic!("expected UUID requires a topic view"));

    assert!(
        host.apply_partitioning_view(request, &topic_view())
            .unwrap_or_else(|error| panic!("apply expected UUID partitioning view: {error:?}"))
    );
    let progress = host
        .drive_waiting(Moment::from_tick(1), 1)
        .unwrap_or_else(|error| panic!("drive expected UUID waiting record: {error:?}"));
    assert_eq!(progress.progressed, 1);
    assert_eq!(host.stats().store.records, 1);
    drop(accepted);
}

#[test]
fn expected_uuid_mismatch_is_fatal_and_definitely_unsent() {
    let mut host = start(partitioning_limits());
    let accepted = host
        .try_admit_waiting(
            Moment::from_tick(0),
            deadline(500),
            expected_explicit_record([7; 16]),
        )
        .unwrap_or_else(|_| panic!("expected-bound waiting admission"));
    let request = host
        .take_partitioning_request()
        .unwrap_or_else(|error| panic!("take mismatched UUID partitioning request: {error:?}"))
        .unwrap_or_else(|| panic!("expected UUID requires a topic view"));
    let changed = topic_view_with_uuid([8; 16]);

    assert!(
        host.apply_partitioning_view(request, &changed)
            .unwrap_or_else(|error| panic!("apply mismatched UUID partitioning view: {error:?}"))
    );
    let (_id, observer, _token) = accepted.into_parts();
    assert_not_sent(observer.wait(), ProducerDeliveryFailureKind::Identity);
}

fn expected_explicit_record(expected_topic_uuid: [u8; 16]) -> ProducerRecord {
    ProducerRecord::from_public(ProducerRecordParts {
        topic: Arc::from("orders"),
        expected_topic_uuid: Some(expected_topic_uuid),
        partition: Some(PartitionIndex::from_raw(2)),
        timestamp_ms: 10,
        defaulted_timestamp: false,
        key: None,
        value: Some(Bytes::from_static(b"value")),
        headers: Vec::new(),
        source_owner: ProducerSourceOwner::none(),
    })
}
