//! Value and translation-boundary tests for multi-group `ShareGroup` results.
#![expect(
    clippy::expect_used,
    reason = "the test asserts exact per-group result variants"
)]

use std::time::Duration;

use crate::{BatchResult, ErrorKind, KafkaError, admin::ShareGroupDescription};

use super::DescribeShareGroupsResult;

fn description(group_id: &str, epoch: i32) -> ShareGroupDescription {
    ShareGroupDescription::new(
        group_id.to_owned(),
        "Stable".to_owned(),
        epoch,
        epoch + 1,
        "uniform".to_owned(),
        Vec::new(),
        None,
    )
}

#[test]
fn result_preserves_max_throttle_caller_order_and_partial_failures() {
    let rejected =
        KafkaError::new(ErrorKind::Broker, "group authorization failed").with_broker_code(Some(30));
    let result = DescribeShareGroupsResult::new(
        Duration::from_millis(37),
        BatchResult::new(vec![
            ("alpha".to_owned(), Ok(description("alpha", 4))),
            ("beta".to_owned(), Err(rejected.clone())),
            ("gamma".to_owned(), Ok(description("gamma", 9))),
        ]),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(37));

    let entries = result.groups().entries();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].0, "alpha");
    assert_eq!(
        entries[0].1.as_ref().expect("alpha description").group_id(),
        "alpha"
    );
    assert_eq!(entries[1].0, "beta");
    assert_eq!(entries[1].1.as_ref().expect_err("beta failure"), &rejected);
    assert_eq!(entries[2].0, "gamma");
    assert_eq!(
        entries[2].1.as_ref().expect("gamma description").group_id(),
        "gamma"
    );
}

#[test]
fn into_groups_preserves_the_translation_boundary_entries() {
    let rejected = KafkaError::new(ErrorKind::Broker, "unknown group").with_broker_code(Some(69));
    let groups = DescribeShareGroupsResult::new(
        Duration::ZERO,
        BatchResult::new(vec![
            ("missing".to_owned(), Err(rejected.clone())),
            ("ready".to_owned(), Ok(description("ready", 2))),
        ]),
    )
    .into_groups()
    .into_entries();

    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].0, "missing");
    assert_eq!(
        groups[0].1.as_ref().expect_err("missing failure"),
        &rejected
    );
    assert_eq!(groups[1].0, "ready");
    assert_eq!(
        groups[1].1.as_ref().expect("ready description").group_id(),
        "ready"
    );
}
