//! Strict one-partition Admin `ListOffsets` response normalization scenarios.

use kafka_client_core::{
    AdminListOffsetResult, AdminListOffsetSpec, AdminListOffsetTarget, ReadIsolation,
};
use kafka_wire::{
    ListOffsetsResponse,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
};

use super::{AdminListOffsetsResponseFailure, normalize_admin_list_offsets_response};

const SELECTED_VERSION: i16 = 11;

#[test]
fn successful_result_preserves_offset_timestamp_and_leader_epoch() {
    let mut partition = partition_response(3, 0, 42);
    partition.timestamp = 1_234;
    partition.leader_epoch = 7;
    let normalized = normalize(&response(vec![topic_response("audit", vec![partition])]))
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let AdminListOffsetResult::Listed(value) = normalized.outcome().result() else {
        panic!("success became failure");
    };
    assert_eq!(value.offset(), Some(42));
    assert_eq!(value.timestamp_ms(), Some(1_234));
    assert_eq!(value.leader_epoch(), Some(7));
}

#[test]
fn successful_absence_sentinels_remain_explicit() {
    let normalized = normalize(&response(vec![topic_response(
        "audit",
        vec![partition_response(3, 0, -1)],
    )]))
    .unwrap_or_else(|error| panic!("valid absence response: {error:?}"));
    let AdminListOffsetResult::Listed(value) = normalized.outcome().result() else {
        panic!("success became failure");
    };
    assert_eq!(value.offset(), None);
    assert_eq!(value.timestamp_ms(), None);
    assert_eq!(value.leader_epoch(), None);
}

#[test]
fn kafka_43_unknown_offset_is_exact_for_sparse_selectors_only() {
    for spec in [
        AdminListOffsetSpec::Timestamp(1_234),
        AdminListOffsetSpec::MaxTimestamp,
        AdminListOffsetSpec::LatestTiered,
        AdminListOffsetSpec::EarliestPendingUpload,
    ] {
        let target = AdminListOffsetTarget::new("audit".to_owned(), 3, spec);
        let normalized = normalize_admin_list_offsets_response(
            &target,
            ReadIsolation::ReadUncommitted,
            SELECTED_VERSION,
            &response(vec![topic_response(
                "audit",
                vec![partition_response(3, 0, -1)],
            )]),
        )
        .unwrap_or_else(|error| panic!("valid unknown offset for {spec:?}: {error:?}"));
        let AdminListOffsetResult::Listed(value) = normalized.outcome().result() else {
            panic!("unknown offset became failure");
        };
        assert_eq!(value.offset(), None);
    }

    for spec in [
        AdminListOffsetSpec::Earliest,
        AdminListOffsetSpec::Latest,
        AdminListOffsetSpec::EarliestLocal,
    ] {
        let target = AdminListOffsetTarget::new("audit".to_owned(), 3, spec);
        assert_eq!(
            normalize_admin_list_offsets_response(
                &target,
                ReadIsolation::ReadUncommitted,
                SELECTED_VERSION,
                &response(vec![topic_response(
                    "audit",
                    vec![partition_response(3, 0, -1)],
                )]),
            ),
            Err(AdminListOffsetsResponseFailure::InvalidOffset { actual: -1 })
        );
    }
}

#[test]
fn selected_version_must_represent_selector_and_isolation() {
    let generated = response(vec![topic_response(
        "audit",
        vec![partition_response(3, 0, 1)],
    )]);
    for (spec, isolation, selected_version) in [
        (AdminListOffsetSpec::Latest, ReadIsolation::ReadCommitted, 1),
        (
            AdminListOffsetSpec::MaxTimestamp,
            ReadIsolation::ReadUncommitted,
            6,
        ),
        (
            AdminListOffsetSpec::EarliestLocal,
            ReadIsolation::ReadUncommitted,
            7,
        ),
        (
            AdminListOffsetSpec::LatestTiered,
            ReadIsolation::ReadUncommitted,
            8,
        ),
        (
            AdminListOffsetSpec::EarliestPendingUpload,
            ReadIsolation::ReadUncommitted,
            10,
        ),
    ] {
        let target = AdminListOffsetTarget::new("audit".to_owned(), 3, spec);
        assert_eq!(
            normalize_admin_list_offsets_response(&target, isolation, selected_version, &generated,),
            Err(AdminListOffsetsResponseFailure::UnsupportedApiVersion {
                actual: selected_version,
            })
        );
    }
}

#[test]
fn unknown_signed_broker_code_is_lossless_before_success_validation() {
    for expected in [-32_000, 1, i16::MAX] {
        let normalized = normalize(&response(vec![topic_response(
            "audit",
            vec![partition_response(3, expected, -2)],
        )]))
        .unwrap_or_else(|error| panic!("broker error remains valid: {error:?}"));
        let AdminListOffsetResult::Failed(error) = normalized.outcome().result() else {
            panic!("broker error became success");
        };
        assert_eq!(error.code(), expected);
    }
}

#[test]
fn throttle_and_selected_version_fields_are_strict() {
    let mut partition = partition_response(3, 0, 42);
    partition.leader_epoch = 7;
    let mut generated = response(vec![topic_response("audit", vec![partition])]);
    generated.throttle_time_ms = 91;

    let v1 = normalize_version(1, &generated)
        .unwrap_or_else(|error| panic!("valid v1 response: {error:?}"));
    assert_eq!(v1.throttle_time_ms(), 0);
    let AdminListOffsetResult::Listed(v1_value) = v1.outcome().result() else {
        panic!("v1 success became failure");
    };
    assert_eq!(v1_value.leader_epoch(), None);

    let v4 = normalize_version(4, &generated)
        .unwrap_or_else(|error| panic!("valid v4 response: {error:?}"));
    assert_eq!(v4.throttle_time_ms(), 91);
    let AdminListOffsetResult::Listed(v4_value) = v4.outcome().result() else {
        panic!("v4 success became failure");
    };
    assert_eq!(v4_value.leader_epoch(), Some(7));

    for actual in [0, 12, i16::MAX] {
        assert_eq!(
            normalize_version(actual, &generated),
            Err(AdminListOffsetsResponseFailure::UnsupportedApiVersion { actual })
        );
    }
}

#[test]
fn topic_and_partition_correlation_are_exact() {
    assert_eq!(
        normalize(&response(Vec::new())),
        Err(AdminListOffsetsResponseFailure::MissingTopic)
    );
    assert_eq!(
        normalize(&response(vec![
            topic_response("audit", vec![partition_response(3, 0, 1)]),
            topic_response("audit", vec![partition_response(3, 0, 1)]),
        ])),
        Err(AdminListOffsetsResponseFailure::DuplicateTopic)
    );
    assert_eq!(
        normalize(&response(vec![topic_response(
            "other",
            vec![partition_response(3, 0, 1)],
        )])),
        Err(AdminListOffsetsResponseFailure::UnexpectedTopic)
    );
    assert_eq!(
        normalize(&response(vec![topic_response("audit", Vec::new())])),
        Err(AdminListOffsetsResponseFailure::MissingPartition)
    );
    assert_eq!(
        normalize(&response(vec![topic_response(
            "audit",
            vec![partition_response(4, 0, 1)],
        )])),
        Err(AdminListOffsetsResponseFailure::UnexpectedPartition { actual: 4 })
    );
}

#[test]
fn invalid_success_ranges_and_negative_throttle_are_not_bound() {
    let mut negative_throttle = response(vec![topic_response(
        "audit",
        vec![partition_response(3, 0, 1)],
    )]);
    negative_throttle.throttle_time_ms = -1;
    assert_eq!(
        normalize(&negative_throttle),
        Err(AdminListOffsetsResponseFailure::NegativeThrottleTime { actual: -1 })
    );

    let invalid_offset = response(vec![topic_response(
        "audit",
        vec![partition_response(3, 0, -2)],
    )]);
    assert_eq!(
        normalize(&invalid_offset),
        Err(AdminListOffsetsResponseFailure::InvalidOffset { actual: -2 })
    );

    let latest = AdminListOffsetTarget::new("audit".to_owned(), 3, AdminListOffsetSpec::Latest);
    let absent = response(vec![topic_response(
        "audit",
        vec![partition_response(3, 0, -1)],
    )]);
    assert_eq!(
        normalize_admin_list_offsets_response(
            &latest,
            ReadIsolation::ReadUncommitted,
            SELECTED_VERSION,
            &absent,
        ),
        Err(AdminListOffsetsResponseFailure::InvalidOffset { actual: -1 })
    );
}

fn target() -> AdminListOffsetTarget {
    AdminListOffsetTarget::new("audit".to_owned(), 3, AdminListOffsetSpec::Timestamp(1_234))
}

fn normalize(
    response: &ListOffsetsResponse,
) -> Result<super::NormalizedAdminListOffsetsResponse, AdminListOffsetsResponseFailure> {
    normalize_version(SELECTED_VERSION, response)
}

fn normalize_version(
    selected_version: i16,
    response: &ListOffsetsResponse,
) -> Result<super::NormalizedAdminListOffsetsResponse, AdminListOffsetsResponseFailure> {
    normalize_admin_list_offsets_response(
        &target(),
        ReadIsolation::ReadUncommitted,
        selected_version,
        response,
    )
}

fn partition_response(
    partition_index: i32,
    error_code: i16,
    offset: i64,
) -> ListOffsetsPartitionResponse {
    let mut partition = ListOffsetsPartitionResponse::default();
    partition.partition_index = partition_index;
    partition.error_code = error_code;
    partition.offset = offset;
    partition
}

fn topic_response(
    name: &str,
    partitions: Vec<ListOffsetsPartitionResponse>,
) -> ListOffsetsTopicResponse {
    let mut topic = ListOffsetsTopicResponse::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}

fn response(topics: Vec<ListOffsetsTopicResponse>) -> ListOffsetsResponse {
    let mut response = ListOffsetsResponse::default();
    response.topics = topics;
    response
}
