//! Malformed topic and partition response rejection scenarios.

use kafka_wire::{OffsetFetchResponse, offset_fetch_response::OffsetFetchResponseGroup};

use super::{
    model_test::{partition, topic},
    response::{GroupOffsetsProtocolFailure, validate_group_offsets_response},
};

#[test]
fn successful_invalid_sentinels_are_rejected_but_broker_errors_stay_exact() {
    let mut invalid = OffsetFetchResponse::default();
    invalid.topics = vec![topic("orders", vec![partition(0, -2, -1, None, 0)])];
    assert_eq!(
        validate_group_offsets_response("readers", &invalid, 7, usize::MAX).err(),
        Some(GroupOffsetsProtocolFailure::InvalidCommittedOffset { actual: -2 })
    );

    invalid.topics[0].partitions[0].error_code = -912;
    assert!(validate_group_offsets_response("readers", &invalid, 7, usize::MAX).is_ok());
}

#[test]
fn unsupported_versions_and_cross_era_fields_are_rejected() {
    for version in [1, 10] {
        assert!(matches!(
            validate_group_offsets_response(
                "readers",
                &OffsetFetchResponse::default(),
                version,
                usize::MAX,
            ),
            Err(GroupOffsetsProtocolFailure::UnsupportedApiVersion { actual }) if actual == version
        ));
    }
    let mut legacy = OffsetFetchResponse::default();
    legacy.groups.push(OffsetFetchResponseGroup::default());
    assert_eq!(
        validate_group_offsets_response("readers", &legacy, 7, usize::MAX).err(),
        Some(GroupOffsetsProtocolFailure::UnexpectedMultiGroupResults)
    );
}
