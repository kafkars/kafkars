//! Executable `ShareFetch` v1 request-shape and session-correlation evidence.

use kafka_wire_core::{ApiVersion, KafkaEncode};

use super::{
    ShareFetchRequestFailure, ShareFetchRequestPlan, ShareFetchRequestSettings,
    ShareFetchRequestTopic, share_fetch_close_request, share_fetch_request,
};

#[test]
fn initial_request_materializes_complete_fetch_only_session() {
    let prepared = share_fetch_request(
        "workers",
        "member-a",
        0,
        settings(),
        plan(vec![topic(1, &[0, 1])], vec![topic(1, &[0, 1])], vec![]),
    )
    .unwrap_or_else(|error| panic!("valid request: {error:?}"));
    let request = prepared.request_for_test();
    assert_eq!(
        request
            .group_id
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("workers")
    );
    assert_eq!(
        request
            .member_id
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("member-a")
    );
    assert_eq!(request.share_session_epoch, 0);
    assert_eq!(request.max_wait_ms, 500);
    assert_eq!(request.min_bytes, 1);
    assert_eq!(request.max_bytes, 1_024);
    assert_eq!(request.max_records, 32);
    assert_eq!(request.batch_size, 8);
    assert_eq!(request.share_acquire_mode, 0);
    assert!(!request.is_renew_ack);
    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].topic_id.to_bytes(), id(1));
    assert_eq!(request.topics[0].partitions.len(), 2);
    assert!(
        request.topics[0]
            .partitions
            .iter()
            .all(|partition| partition.acknowledgement_batches.is_empty())
    );
    assert!(request.forgotten_topics_data.is_empty());
    assert!(request.encoded_len(ApiVersion::new(1)).is_ok());
}

#[test]
fn incremental_request_retains_full_correlation_but_sends_only_changes() {
    let prepared = share_fetch_request(
        "workers",
        "member-a",
        4,
        settings(),
        plan(
            vec![topic(1, &[1])],
            vec![topic(1, &[1])],
            vec![topic(1, &[0])],
        ),
    )
    .unwrap_or_else(|error| panic!("valid incremental request: {error:?}"));
    let (request, correlation) = prepared.into_parts();
    assert_eq!(request.share_session_epoch, 4);
    assert_eq!(request.topics[0].partitions[0].partition_index, 1);
    assert_eq!(request.forgotten_topics_data[0].partitions, vec![0]);
    assert!(correlation.contains(id(1), 1));
    assert!(!correlation.contains(id(1), 0));
}

#[test]
fn final_request_closes_the_session_without_fetch_or_acknowledgement_data() {
    let prepared = share_fetch_close_request("workers", "member-a")
        .unwrap_or_else(|error| panic!("valid close request: {error:?}"));
    let (request, correlation) = prepared.into_parts();
    assert_eq!(request.share_session_epoch, -1);
    assert!(request.topics.is_empty());
    assert!(request.forgotten_topics_data.is_empty());
    assert!(!correlation.contains(id(1), 0));
    assert!(request.encoded_len(ApiVersion::new(1)).is_ok());
}

#[test]
fn invalid_identity_bounds_and_session_deltas_fail_before_generated_ownership() {
    assert_eq!(
        share_fetch_request("", "member-a", 0, settings(), plan(vec![], vec![], vec![])).err(),
        Some(ShareFetchRequestFailure::GroupId)
    );
    assert_eq!(
        share_fetch_request(
            "workers",
            "member-a",
            -1,
            settings(),
            plan(vec![], vec![], vec![]),
        )
        .err(),
        Some(ShareFetchRequestFailure::SessionEpoch(-1))
    );
    assert_eq!(
        ShareFetchRequestTopic::try_new([0; 16], vec![0]),
        Err(ShareFetchRequestFailure::ZeroTopicId)
    );
    assert_eq!(
        ShareFetchRequestTopic::try_new(id(1), vec![0, 0]),
        Err(ShareFetchRequestFailure::DuplicatePartition(0))
    );
    let invalid_initial = share_fetch_request(
        "workers",
        "member-a",
        0,
        settings(),
        plan(vec![topic(1, &[0, 1])], vec![topic(1, &[0])], vec![]),
    );
    assert_eq!(
        invalid_initial.err(),
        Some(ShareFetchRequestFailure::InitialRequestShape)
    );
}

fn settings() -> ShareFetchRequestSettings {
    ShareFetchRequestSettings {
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1_024,
        max_records: 32,
        batch_size: 8,
    }
}

fn plan(
    active: Vec<ShareFetchRequestTopic>,
    included: Vec<ShareFetchRequestTopic>,
    forgotten: Vec<ShareFetchRequestTopic>,
) -> ShareFetchRequestPlan {
    ShareFetchRequestPlan::try_new(active, included, forgotten)
        .unwrap_or_else(|error| panic!("valid plan: {error:?}"))
}

fn topic(value: u8, partitions: &[u32]) -> ShareFetchRequestTopic {
    ShareFetchRequestTopic::try_new(id(value), partitions.to_vec())
        .unwrap_or_else(|error| panic!("valid topic: {error:?}"))
}

fn id(value: u8) -> [u8; 16] {
    let mut id = [0; 16];
    id[0] = value;
    id
}
