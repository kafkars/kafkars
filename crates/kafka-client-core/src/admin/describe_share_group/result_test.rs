//! Scenarios for exact generated-free API-77 values.

use core::num::NonZeroI16;

use super::{
    DescribeShareGroupAssignment, DescribeShareGroupBrokerError, DescribeShareGroupDescription,
    DescribeShareGroupMember, DescribeShareGroupResult, DescribeShareGroupTopicAssignment,
};

#[test]
fn description_retains_exact_epochs_member_and_assignment_facts() {
    let description = description();
    let result = DescribeShareGroupResult::new(73, description);

    assert_eq!(result.throttle_time_ms(), 73);
    assert_eq!(result.description().group_id(), "share-workers");
    assert_eq!(result.description().group_epoch(), 1);
    assert_eq!(result.description().assignment_epoch(), 9);
    assert_eq!(result.description().members()[0].member_id(), "member-a");
    assert_eq!(
        result.description().members()[0].assignment().topics()[0].topic_id(),
        &[7; 16]
    );
}

#[test]
fn broker_error_retains_throttle_code_and_nullable_diagnostic() {
    let error = DescribeShareGroupBrokerError::new(
        41,
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero")),
        Some("group prefix".to_owned()),
        false,
    );

    assert_eq!(
        error.into_parts(),
        (41, -32_000, Some("group prefix".to_owned()), false)
    );
}

fn description() -> DescribeShareGroupDescription {
    DescribeShareGroupDescription::new(
        "share-workers".to_owned(),
        "Stable".to_owned(),
        1,
        9,
        "uniform".to_owned(),
        vec![DescribeShareGroupMember::new(
            "member-a".to_owned(),
            Some("rack-a".to_owned()),
            4,
            "client-a".to_owned(),
            "/127.0.0.1".to_owned(),
            vec!["orders".to_owned()],
            DescribeShareGroupAssignment::new(vec![DescribeShareGroupTopicAssignment::new(
                [7; 16],
                "orders".to_owned(),
                vec![0, 1],
            )]),
        )],
        Some(3),
    )
}
