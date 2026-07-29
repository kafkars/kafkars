//! Error-level, selected-version, diagnostic, and capacity evidence.

use kafka_wire::{
    DescribeQuorumResponse,
    describe_quorum_response::{PartitionData, TopicData},
};

use super::{
    DescribeMetadataQuorumProtocolFailure, NormalizedMetadataQuorumOutcome,
    normalize_describe_metadata_quorum_response, request::METADATA_TOPIC,
};

const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn top_level_error_needs_no_topic_payload_and_preserves_exact_code() {
    let mut response = DescribeQuorumResponse::default();
    response.error_code = -37;
    response.error_message = Some("not-controller".into());
    let normalized = normalize_describe_metadata_quorum_response(2, &response, LIMIT)
        .unwrap_or_else(|error| panic!("valid top-level error: {error:?}"));
    let (outcome, retained) = normalized.into_parts();
    assert!(retained > 0);
    let NormalizedMetadataQuorumOutcome::TopLevelError(error) = outcome else {
        panic!("expected top-level rejection");
    };
    assert_eq!(
        error.into_parts(),
        (-37, Some("not-controller".to_owned()), false)
    );
}

#[test]
fn older_selected_version_hides_unrepresentable_error_diagnostic() {
    let mut response = DescribeQuorumResponse::default();
    response.error_code = 42;
    response.error_message = Some("generated-only-default".into());
    let normalized = normalize_describe_metadata_quorum_response(1, &response, LIMIT)
        .unwrap_or_else(|error| panic!("valid v1 top-level error: {error:?}"));
    let (outcome, _) = normalized.into_parts();
    let NormalizedMetadataQuorumOutcome::TopLevelError(error) = outcome else {
        panic!("expected top-level rejection");
    };
    assert_eq!(error.into_parts(), (42, None, false));
}

#[test]
fn partition_error_is_distinct_and_skips_success_only_validation() {
    let mut partition = PartitionData::default();
    partition.partition_index = 0;
    partition.error_code = 71;
    partition.error_message = Some("partition-failed".into());
    partition.leader_id = -99;
    partition.leader_epoch = -99;
    partition.high_watermark = -99;

    let mut topic = TopicData::default();
    topic.topic_name = METADATA_TOPIC.into();
    topic.partitions = vec![partition];
    let mut response = DescribeQuorumResponse::default();
    response.topics = vec![topic];

    let normalized = normalize_describe_metadata_quorum_response(2, &response, LIMIT)
        .unwrap_or_else(|error| panic!("valid partition rejection: {error:?}"));
    let (outcome, _) = normalized.into_parts();
    let NormalizedMetadataQuorumOutcome::PartitionError(error) = outcome else {
        panic!("expected partition rejection");
    };
    assert_eq!(
        error.into_parts(),
        (71, Some("partition-failed".to_owned()), false)
    );
}

#[test]
fn utf8_diagnostic_is_bounded_and_reports_truncation() {
    let mut response = DescribeQuorumResponse::default();
    response.error_code = 1;
    response.error_message = Some(format!("{}é", "x".repeat(1023)).into());
    let normalized = normalize_describe_metadata_quorum_response(2, &response, LIMIT)
        .unwrap_or_else(|error| panic!("bounded diagnostic: {error:?}"));
    let (outcome, _) = normalized.into_parts();
    let NormalizedMetadataQuorumOutcome::TopLevelError(error) = outcome else {
        panic!("expected top-level rejection");
    };
    let (_, message, truncated) = error.into_parts();
    let expected = "x".repeat(1023);
    assert_eq!(message.as_deref(), Some(expected.as_str()));
    assert!(truncated);
}

#[test]
fn unsupported_version_and_error_retention_are_explicit() {
    let mut response = DescribeQuorumResponse::default();
    response.error_code = 1;
    assert_eq!(
        normalize_describe_metadata_quorum_response(3, &response, LIMIT),
        Err(DescribeMetadataQuorumProtocolFailure::UnsupportedApiVersion { actual: 3 })
    );
    assert!(matches!(
        normalize_describe_metadata_quorum_response(2, &response, 0),
        Err(DescribeMetadataQuorumProtocolFailure::RetainedBytes { .. })
    ));
}
