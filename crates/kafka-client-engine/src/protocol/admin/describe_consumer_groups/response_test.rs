//! Strict singleton group-response normalization scenarios.

use kafka_client_core::{
    AdminConsumerGroupDescriptionDetails, AdminConsumerGroupDescriptionResult,
    AdminConsumerGroupMemberDetails,
};
use kafka_wire::{
    DescribeGroupsResponse,
    describe_groups_response::{DescribedGroup, DescribedGroupMember},
};

use super::{DescribeConsumerGroupResponseFailure, normalize_describe_consumer_group_response};

#[test]
fn success_preserves_scalar_raw_member_and_authorization_facts() {
    let mut first = DescribedGroupMember::default();
    first.member_id = "z-member".into();
    first.group_instance_id = Some("instance-z".into());
    first.client_id = "client-z".into();
    first.client_host = "host-z".into();
    first.member_metadata = vec![1, 2].into();
    first.member_assignment = vec![3, 4].into();
    let mut second = DescribedGroupMember::default();
    second.member_id = "a-member".into();
    let mut group = group("workers");
    group.group_state = "Stable".into();
    group.protocol_type = "consumer".into();
    group.protocol_data = "range".into();
    group.members = vec![first, second];
    group.authorized_operations = 0x23;
    let normalized = normalize_describe_consumer_group_response(
        "workers",
        true,
        6,
        &response(group),
        4 * 1024 * 1024,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let (throttle, outcome, retained_bytes) = normalized.into_parts();
    assert_eq!(throttle, 9);
    assert!(retained_bytes > 0);
    let (_, AdminConsumerGroupDescriptionResult::Described(description)) = outcome.into_parts()
    else {
        panic!("success became failure");
    };
    let (state, details, members, operations) = description.into_parts();
    assert_eq!(state, "Stable");
    let AdminConsumerGroupDescriptionDetails::Classic(details) = details else {
        panic!("classic response changed protocol kind");
    };
    let (protocol_type, protocol_data) = details.into_parts();
    assert_eq!(protocol_type, "consumer");
    assert_eq!(protocol_data, "range");
    assert_eq!(operations, Some(0x23));
    let (member_id, instance_id, client_id, host, details) = members[1].clone().into_parts();
    assert_eq!(member_id, "z-member");
    assert_eq!(instance_id.as_deref(), Some("instance-z"));
    assert_eq!(client_id, "client-z");
    assert_eq!(host, "host-z");
    let AdminConsumerGroupMemberDetails::Classic(details) = details else {
        panic!("classic member changed protocol kind");
    };
    assert_eq!(details.into_parts(), (vec![1, 2], vec![3, 4]));
}

#[test]
fn correlation_compatibility_and_retained_capacity_are_strict() {
    assert_eq!(
        normalize_describe_consumer_group_response(
            "workers",
            false,
            6,
            &DescribeGroupsResponse::default(),
            usize::MAX,
        ),
        Err(DescribeConsumerGroupResponseFailure::MissingGroup)
    );
    assert_eq!(
        normalize_describe_consumer_group_response(
            "workers",
            false,
            6,
            &response(group("other")),
            usize::MAX,
        ),
        Err(DescribeConsumerGroupResponseFailure::UnexpectedGroup)
    );
    assert_eq!(
        normalize_describe_consumer_group_response(
            "workers",
            true,
            2,
            &response(group("workers")),
            usize::MAX,
        ),
        Err(DescribeConsumerGroupResponseFailure::AuthorizedOperationsUnavailable { actual: 2 })
    );
    assert_eq!(
        normalize_describe_consumer_group_response(
            "workers",
            false,
            6,
            &response(group("workers")),
            1,
        ),
        Err(DescribeConsumerGroupResponseFailure::RetainedBytes)
    );
}

fn group(group_id: &str) -> DescribedGroup {
    let mut group = DescribedGroup::default();
    group.group_id = group_id.into();
    group
}

fn response(group: DescribedGroup) -> DescribeGroupsResponse {
    let mut response = DescribeGroupsResponse::default();
    response.throttle_time_ms = 9;
    response.groups = vec![group];
    response
}
