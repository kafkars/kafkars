//! Scenarios for inert engine Admin `ListOffsets` requests.

use kafka_client_core::AdminListOffsetSpec;

use kafka_client_core::ReadIsolation;

use crate::config::ConsumerReadIsolation;

use super::{AdminListOffsetsRequest, AdminListOffsetsRequestSpec, AdminListOffsetsRequestTarget};

#[test]
fn request_preserves_caller_order_and_specs_until_core_validation() {
    let request = AdminListOffsetsRequest::new(vec![
        target("orders", 2, AdminListOffsetsRequestSpec::Latest).current_leader_epoch(17),
        target(
            "audit",
            0,
            AdminListOffsetsRequestSpec::Timestamp(1_700_000_000_123),
        ),
    ])
    .canonicalize();
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.targets()[0].topic(), "orders");
    assert_eq!(plan.targets()[0].spec(), AdminListOffsetSpec::Latest);
    assert_eq!(plan.targets()[0].current_leader_epoch(), Some(17));
    assert_eq!(plan.targets()[1].topic(), "audit");
    assert_eq!(plan.targets()[1].current_leader_epoch(), None);
    assert_eq!(
        plan.targets()[1].spec(),
        AdminListOffsetSpec::Timestamp(1_700_000_000_123)
    );
    assert_eq!(plan.read_isolation(), ReadIsolation::ReadUncommitted);
}

#[test]
fn request_preserves_new_selectors_and_explicit_read_isolation() {
    let plan = AdminListOffsetsRequest::new(vec![
        target("max", 0, AdminListOffsetsRequestSpec::MaxTimestamp),
        target("local", 1, AdminListOffsetsRequestSpec::EarliestLocal),
        target("tiered", 2, AdminListOffsetsRequestSpec::LatestTiered),
        target(
            "pending",
            3,
            AdminListOffsetsRequestSpec::EarliestPendingUpload,
        ),
    ])
    .with_read_isolation(ConsumerReadIsolation::ReadCommitted)
    .into_plan()
    .unwrap_or_else(|error| panic!("valid new-selector request: {error}"));

    assert_eq!(plan.read_isolation(), ReadIsolation::ReadCommitted);
    assert_eq!(plan.targets()[0].spec(), AdminListOffsetSpec::MaxTimestamp);
    assert_eq!(plan.targets()[1].spec(), AdminListOffsetSpec::EarliestLocal);
    assert_eq!(plan.targets()[2].spec(), AdminListOffsetSpec::LatestTiered);
    assert_eq!(
        plan.targets()[3].spec(),
        AdminListOffsetSpec::EarliestPendingUpload
    );
}

#[test]
fn invalid_scalar_facts_remain_inert_until_plan_conversion() {
    let request = AdminListOffsetsRequest::new(vec![target(
        "orders",
        -1,
        AdminListOffsetsRequestSpec::Timestamp(-1),
    )]);
    assert!(request.into_plan().is_err());

    let stale_epoch = AdminListOffsetsRequest::new(vec![
        target("orders", 0, AdminListOffsetsRequestSpec::Latest).current_leader_epoch(-1),
    ]);
    assert!(stale_epoch.into_plan().is_err());
}

fn target(
    topic: &str,
    partition: i32,
    spec: AdminListOffsetsRequestSpec,
) -> AdminListOffsetsRequestTarget {
    AdminListOffsetsRequestTarget::new(topic.to_owned(), partition, spec)
}
