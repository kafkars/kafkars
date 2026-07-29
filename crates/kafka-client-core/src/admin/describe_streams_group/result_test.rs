//! Scenarios for exact generated-free API-89 values.

use core::num::NonZeroI16;

use super::{
    DescribeStreamsGroupAssignment, DescribeStreamsGroupBrokerError,
    DescribeStreamsGroupDescription, DescribeStreamsGroupMember, DescribeStreamsGroupResult,
    DescribeStreamsGroupTaskIds, DescribeStreamsGroupTopologyDescriptionStatus,
};

#[test]
fn description_retains_exact_epochs_member_tasks_and_raw_status() {
    let result = DescribeStreamsGroupResult::new(89, description());

    assert_eq!(result.throttle_time_ms(), 89);
    assert_eq!(result.description().group_id(), "streams-workers");
    assert_eq!(result.description().group_epoch(), 1);
    assert_eq!(result.description().assignment_epoch(), 9);
    assert_eq!(result.description().members()[0].member_id(), "member-a");
    assert_eq!(
        result
            .description()
            .topology_description_status()
            .map(DescribeStreamsGroupTopologyDescriptionStatus::raw),
        Some(11)
    );
}

#[test]
fn broker_error_retains_throttle_code_and_nullable_diagnostic() {
    let error = DescribeStreamsGroupBrokerError::new(
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

fn description() -> DescribeStreamsGroupDescription {
    let assignment = DescribeStreamsGroupAssignment::new(
        vec![DescribeStreamsGroupTaskIds::new(
            "subtopology".to_owned(),
            vec![0, 1],
        )],
        Vec::new(),
        Vec::new(),
    );
    DescribeStreamsGroupDescription::new(
        "streams-workers".to_owned(),
        "Stable".to_owned(),
        1,
        9,
        None,
        vec![DescribeStreamsGroupMember::new(
            "member-a".to_owned(),
            4,
            Some("instance-a".to_owned()),
            Some("rack-a".to_owned()),
            "client-a".to_owned(),
            "/127.0.0.1".to_owned(),
            3,
            "process-a".to_owned(),
            None,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            assignment,
            DescribeStreamsGroupAssignment::new(Vec::new(), Vec::new(), Vec::new()),
            false,
        )],
        Some(3),
        None,
        Some(DescribeStreamsGroupTopologyDescriptionStatus::new(11)),
    )
}
