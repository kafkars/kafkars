//! Scenarios for Admin `ListOffsets` intent validation.

use crate::ReadIsolation;

use super::{
    AdminListOffsetSpec, AdminListOffsetTarget, AdminListOffsetsPlan, AdminListOffsetsPlanError,
};

#[test]
fn plan_preserves_caller_order_and_each_offset_specification() {
    let plan = AdminListOffsetsPlan::new(vec![
        target("orders", 2, AdminListOffsetSpec::Latest).with_current_leader_epoch(Some(7)),
        target(
            "audit",
            0,
            AdminListOffsetSpec::Timestamp(1_700_000_000_000),
        ),
        target("orders", 1, AdminListOffsetSpec::Earliest),
    ])
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.targets()[0].topic(), "orders");
    assert_eq!(plan.targets()[0].partition(), 2);
    assert_eq!(plan.targets()[0].spec(), AdminListOffsetSpec::Latest);
    assert_eq!(plan.targets()[0].current_leader_epoch(), Some(7));
    assert_eq!(
        plan.targets()[1].spec(),
        AdminListOffsetSpec::Timestamp(1_700_000_000_000)
    );
    assert_eq!(plan.targets()[1].current_leader_epoch(), None);
    assert_eq!(plan.targets()[2].spec(), AdminListOffsetSpec::Earliest);
    assert_eq!(plan.read_isolation(), ReadIsolation::ReadUncommitted);
}

#[test]
fn absent_epoch_preserves_existing_version_floors() {
    for (spec, read_isolation, expected) in [
        (
            AdminListOffsetSpec::Earliest,
            ReadIsolation::ReadUncommitted,
            1,
        ),
        (AdminListOffsetSpec::Latest, ReadIsolation::ReadCommitted, 2),
        (
            AdminListOffsetSpec::Timestamp(1),
            ReadIsolation::ReadUncommitted,
            1,
        ),
        (
            AdminListOffsetSpec::MaxTimestamp,
            ReadIsolation::ReadUncommitted,
            7,
        ),
        (
            AdminListOffsetSpec::EarliestLocal,
            ReadIsolation::ReadUncommitted,
            8,
        ),
        (
            AdminListOffsetSpec::LatestTiered,
            ReadIsolation::ReadUncommitted,
            9,
        ),
        (
            AdminListOffsetSpec::EarliestPendingUpload,
            ReadIsolation::ReadUncommitted,
            11,
        ),
    ] {
        let target = target("orders", 0, spec);
        assert_eq!(
            target.minimum_api_version(read_isolation),
            expected,
            "{spec:?} under {read_isolation:?}"
        );
    }
}

#[test]
fn present_epoch_raises_only_lower_version_floors_to_v4() {
    for (spec, read_isolation, expected) in [
        (
            AdminListOffsetSpec::Earliest,
            ReadIsolation::ReadUncommitted,
            4,
        ),
        (AdminListOffsetSpec::Latest, ReadIsolation::ReadCommitted, 4),
        (
            AdminListOffsetSpec::MaxTimestamp,
            ReadIsolation::ReadUncommitted,
            7,
        ),
    ] {
        let target = target("orders", 0, spec).with_current_leader_epoch(Some(0));
        assert_eq!(
            target.minimum_api_version(read_isolation),
            expected,
            "{spec:?} under {read_isolation:?}"
        );
    }
}

#[test]
fn plan_preserves_explicit_read_isolation() {
    let plan = AdminListOffsetsPlan::with_read_isolation(
        vec![target("orders", 2, AdminListOffsetSpec::MaxTimestamp)],
        ReadIsolation::ReadCommitted,
    )
    .unwrap_or_else(|error| panic!("valid read-committed plan: {error}"));

    assert_eq!(plan.read_isolation(), ReadIsolation::ReadCommitted);
}

#[test]
fn plan_rejects_empty_invalid_or_duplicate_targets() {
    for (targets, expected) in [
        (Vec::new(), AdminListOffsetsPlanError::EmptyTargetBatch),
        (
            vec![target("", 0, AdminListOffsetSpec::Latest)],
            AdminListOffsetsPlanError::EmptyTopicName,
        ),
        (
            vec![target(&"t".repeat(250), 0, AdminListOffsetSpec::Latest)],
            AdminListOffsetsPlanError::TopicNameTooLong,
        ),
        (
            vec![target("orders", -1, AdminListOffsetSpec::Latest)],
            AdminListOffsetsPlanError::NegativePartition,
        ),
        (
            vec![target("orders", 0, AdminListOffsetSpec::Timestamp(-1))],
            AdminListOffsetsPlanError::NegativeTimestamp,
        ),
        (
            vec![
                target("orders", 0, AdminListOffsetSpec::Latest)
                    .with_current_leader_epoch(Some(-1)),
            ],
            AdminListOffsetsPlanError::NegativeCurrentLeaderEpoch,
        ),
        (
            vec![
                target("orders", 0, AdminListOffsetSpec::Earliest),
                target("orders", 0, AdminListOffsetSpec::Latest),
            ],
            AdminListOffsetsPlanError::DuplicateTopicPartition,
        ),
    ] {
        assert_eq!(AdminListOffsetsPlan::new(targets), Err(expected));
    }
}

fn target(topic: &str, partition: i32, spec: AdminListOffsetSpec) -> AdminListOffsetTarget {
    AdminListOffsetTarget::new(topic.to_owned(), partition, spec)
}
