//! Executable v1 request shape, acknowledgement, and fence evidence.

use kafka_wire_core::{ApiVersion, KafkaEncode, StrBytes};

use super::{
    ShareAcknowledgeRequestFailure,
    request::share_acknowledge_request,
    test_support::{id, prepared_acknowledgement},
};

#[test]
fn request_materializes_exact_session_and_normalized_decisions() {
    let (attempt, acknowledgement) = prepared_acknowledgement();
    let prepared = share_acknowledge_request("workers", "member-a", attempt, &acknowledgement)
        .unwrap_or_else(|error| panic!("valid request: {error:?}"));
    let request = prepared.request_for_test();
    assert_eq!(
        request.group_id.as_ref().map(StrBytes::as_str),
        Some("workers")
    );
    assert_eq!(
        request.member_id.as_ref().map(StrBytes::as_str),
        Some("member-a")
    );
    assert_eq!(request.share_session_epoch, 1);
    assert!(!request.is_renew_ack);
    assert_eq!(request.topics.len(), 2);
    assert_eq!(request.topics[0].topic_id.to_bytes(), id(1));
    assert_eq!(request.topics[0].partitions.len(), 2);
    assert_eq!(request.topics[0].partitions[0].partition_index, 0);
    let first = &request.topics[0].partitions[0].acknowledgement_batches[0];
    assert_eq!((first.first_offset, first.last_offset), (0, 2));
    assert_eq!(first.acknowledge_types, vec![1, 2, 3]);
    assert_eq!(
        request.topics[0].partitions[1].acknowledgement_batches[0].acknowledge_types,
        vec![1]
    );
    assert_eq!(request.topics[1].topic_id.to_bytes(), id(2));
    assert_eq!(
        request.topics[1].partitions[0].acknowledgement_batches[0].acknowledge_types,
        vec![3]
    );
    assert!(request.encoded_len(ApiVersion::new(1)).is_ok());
}

#[test]
fn correlation_retains_every_partition_in_canonical_order() {
    let (attempt, acknowledgement) = prepared_acknowledgement();
    let prepared = share_acknowledge_request("workers", "member-a", attempt, &acknowledgement)
        .unwrap_or_else(|error| panic!("valid request: {error:?}"));
    let (_request, correlation) = prepared.into_parts();
    assert!(correlation.contains(id(1), 0));
    assert!(correlation.contains(id(1), 1));
    assert!(correlation.contains(id(2), 0));
    assert!(!correlation.contains(id(2), 1));
}

#[test]
fn invalid_identity_fails_before_generated_ownership() {
    let (attempt, acknowledgement) = prepared_acknowledgement();
    assert_eq!(
        share_acknowledge_request("", "member-a", attempt, &acknowledgement).err(),
        Some(ShareAcknowledgeRequestFailure::GroupId)
    );
    assert_eq!(
        share_acknowledge_request("workers", "", attempt, &acknowledgement).err(),
        Some(ShareAcknowledgeRequestFailure::MemberId)
    );
}
