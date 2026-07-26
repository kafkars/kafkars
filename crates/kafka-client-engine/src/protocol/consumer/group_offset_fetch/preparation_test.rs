//! Local assignment validation and empty-assignment request suppression.

use std::sync::Arc;

use super::{
    model::GroupOffsetFetchTopic,
    preparation::{
        GroupOffsetFetchPreparation, GroupOffsetFetchRequestPreparationFailure,
        prepare_group_offset_fetch_request,
    },
};

#[test]
fn empty_assignment_is_an_explicit_local_no_request() {
    let prepared = prepare_group_offset_fetch_request(Arc::from("readers"), Vec::new(), 0)
        .unwrap_or_else(|error| panic!("empty assignment is valid: {error:?}"));
    assert!(matches!(prepared, GroupOffsetFetchPreparation::NoRequest));
}

#[test]
fn prepared_request_separates_wire_ownership_from_exact_correlation() {
    let topics = vec![topic("z", &[2, 0]), topic("a", &[1])];
    let GroupOffsetFetchPreparation::Prepared(prepared) =
        prepare_group_offset_fetch_request(Arc::from("readers"), topics, usize::MAX)
            .unwrap_or_else(|error| panic!("valid assignment: {error:?}"))
    else {
        panic!("nonempty assignment must prepare a request");
    };
    let (correlation, request) = prepared.into_parts();

    assert_eq!(correlation.group_id(), "readers");
    assert_eq!(correlation.partition_count(), 3);
    assert_eq!(correlation.topics()[0].partition_indexes(), [2, 0]);
    let _wire = request.into_wire_request();
}

#[test]
fn malformed_assignment_is_rejected_before_wire_preparation() {
    let cases = [
        (
            vec![topic("", &[0])],
            GroupOffsetFetchRequestPreparationFailure::EmptyTopic,
        ),
        (
            vec![topic("a", &[])],
            GroupOffsetFetchRequestPreparationFailure::EmptyTopicPartitions,
        ),
        (
            vec![topic("a", &[0]), topic("a", &[1])],
            GroupOffsetFetchRequestPreparationFailure::DuplicateTopic,
        ),
        (
            vec![topic("a", &[-2])],
            GroupOffsetFetchRequestPreparationFailure::NegativePartition { actual: -2 },
        ),
        (
            vec![topic("a", &[3, 3])],
            GroupOffsetFetchRequestPreparationFailure::DuplicatePartition { actual: 3 },
        ),
    ];
    for (topics, expected) in cases {
        assert_eq!(
            prepare_group_offset_fetch_request(Arc::from("readers"), topics, usize::MAX).err(),
            Some(expected)
        );
    }
    assert_eq!(
        prepare_group_offset_fetch_request(Arc::from(""), vec![topic("a", &[0])], usize::MAX,)
            .err(),
        Some(GroupOffsetFetchRequestPreparationFailure::EmptyGroup)
    );
}

#[test]
fn request_charge_is_exactly_exposed_and_enforced() {
    let GroupOffsetFetchPreparation::Prepared(prepared) = prepare_group_offset_fetch_request(
        Arc::from("readers"),
        vec![topic("z", &[2, 0]), topic("a", &[1])],
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("measure request charge: {error:?}")) else {
        panic!("nonempty assignment must prepare");
    };
    let (_, request) = prepared.into_parts();
    let required = request.retained_bytes();
    assert!(required > "readers".len() + "z".len() + "a".len());

    assert_eq!(
        prepare_group_offset_fetch_request(
            Arc::from("readers"),
            vec![topic("z", &[2, 0]), topic("a", &[1])],
            required - 1,
        )
        .err(),
        Some(GroupOffsetFetchRequestPreparationFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    assert!(matches!(
        prepare_group_offset_fetch_request(
            Arc::from("readers"),
            vec![topic("z", &[2, 0]), topic("a", &[1])],
            required,
        ),
        Ok(GroupOffsetFetchPreparation::Prepared(_))
    ));
}

pub(super) fn topic(name: &str, partitions: &[i32]) -> GroupOffsetFetchTopic {
    GroupOffsetFetchTopic::new(Arc::from(name), partitions.to_vec())
}
