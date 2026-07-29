//! Exact v1 correlation, ordering, errors, topic IDs, and bounds.

use kafka_wire::{
    ShareGroupDescribeResponse,
    share_group_describe_response::{Assignment, DescribedGroup, Member, TopicPartitions},
};
use kafka_wire_core::{StrBytes, Uuid};

use super::{
    DescribeShareGroupProtocolFailure, DescribeShareGroupResult,
    normalize_describe_share_group_response,
};

const RESULT_LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn success_is_singleton_correlated_and_canonicalized_with_exact_topic_ids() {
    let mut response = ShareGroupDescribeResponse::default();
    response.throttle_time_ms = 17;
    let mut group = success_group();
    group.authorized_operations = 3;
    group.members = vec![
        member(
            "zeta",
            vec!["z-topic", "a-topic"],
            vec![
                assigned_topic("z-topic", [2; 16], vec![2, 0]),
                assigned_topic("a-topic", [1; 16], vec![3, 1]),
            ],
        ),
        member("alpha", vec![], vec![]),
    ];
    response.groups = vec![group];

    let normalized = normalize_describe_share_group_response(
        "share-readers",
        true,
        Some(1),
        &response,
        RESULT_LIMIT,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let (throttle, group_id, DescribeShareGroupResult::Described(description), retained) =
        normalized.into_parts()
    else {
        panic!("description expected");
    };
    assert_eq!(throttle, 17);
    assert_eq!(group_id, "share-readers");
    assert!(retained > 0);
    let (state, group_epoch, assignment_epoch, assignor, members, authorized) =
        description.into_parts();
    assert_eq!(state, "Stable");
    assert_eq!(group_epoch, 8);
    assert_eq!(assignment_epoch, 7);
    assert_eq!(assignor, "share");
    assert_eq!(authorized, Some(3));
    assert_eq!(members[0].member_id(), "alpha");
    let (_, _, _, _, _, subscriptions, assignment) = members
        .into_iter()
        .nth(1)
        .unwrap_or_else(|| panic!("zeta"))
        .into_parts();
    assert_eq!(subscriptions, ["a-topic", "z-topic"]);
    assert_eq!(assignment.topics()[0].topic_id(), &[1; 16]);
    assert_eq!(assignment.topics()[0].topic_name(), "a-topic");
    let (_, _, partitions) = assignment.into_topics().remove(0).into_parts();
    assert_eq!(partitions, [1, 3]);
}

#[test]
fn exact_group_error_preserves_signed_code_and_utf8_safe_diagnostic() {
    let mut response = ShareGroupDescribeResponse::default();
    let mut group = DescribedGroup::default();
    group.group_id = "share-readers".into();
    group.error_code = -32000;
    group.error_message = Some(StrBytes::from(format!("{}é", "x".repeat(1023))));
    response.groups = vec![group];

    let normalized = normalize_describe_share_group_response(
        "share-readers",
        false,
        Some(1),
        &response,
        RESULT_LIMIT,
    )
    .unwrap_or_else(|error| panic!("valid error: {error:?}"));
    let (_, _, DescribeShareGroupResult::Failed(error), _) = normalized.into_parts() else {
        panic!("broker error expected");
    };
    let (code, message, truncated) = error.into_parts();
    assert_eq!(code, -32000);
    assert_eq!(message.as_deref().map(str::len), Some(1023));
    assert!(truncated);
}

#[test]
fn version_throttle_and_singleton_correlation_fail_closed() {
    let mut response = ShareGroupDescribeResponse::default();
    response.groups = vec![success_group()];
    assert_eq!(
        normalize_describe_share_group_response(
            "share-readers",
            false,
            Some(0),
            &response,
            RESULT_LIMIT,
        ),
        Err(DescribeShareGroupProtocolFailure::UnsupportedApiVersion { actual: 0 })
    );
    response.throttle_time_ms = -1;
    assert_eq!(
        normalize_describe_share_group_response(
            "share-readers",
            false,
            Some(1),
            &response,
            RESULT_LIMIT,
        ),
        Err(DescribeShareGroupProtocolFailure::NegativeThrottleTime { actual: -1 })
    );
    response.throttle_time_ms = 0;
    response.groups[0].group_id = "other".into();
    assert_eq!(
        normalize_describe_share_group_response(
            "share-readers",
            false,
            Some(1),
            &response,
            RESULT_LIMIT,
        ),
        Err(DescribeShareGroupProtocolFailure::UnexpectedGroup)
    );
    response.groups = Vec::new();
    assert_eq!(
        normalize_describe_share_group_response(
            "share-readers",
            false,
            Some(1),
            &response,
            RESULT_LIMIT,
        ),
        Err(DescribeShareGroupProtocolFailure::MissingGroup)
    );
}

#[test]
fn hostile_member_assignment_and_capacity_shapes_fail_closed() {
    let mut response = ShareGroupDescribeResponse::default();
    let mut group = success_group();
    group.members = vec![member(
        "member",
        vec!["orders"],
        vec![assigned_topic("orders", [0; 16], vec![0])],
    )];
    response.groups = vec![group];
    assert_eq!(
        normalize_describe_share_group_response(
            "share-readers",
            false,
            Some(1),
            &response,
            RESULT_LIMIT,
        ),
        Err(DescribeShareGroupProtocolFailure::ZeroTopicId)
    );

    response.groups[0].members[0].assignment.topic_partitions[0].topic_id =
        Uuid::from_bytes([1; 16]);
    response.groups[0].members[0].assignment.topic_partitions[0].partitions = vec![0, 0];
    assert_eq!(
        normalize_describe_share_group_response(
            "share-readers",
            false,
            Some(1),
            &response,
            RESULT_LIMIT,
        ),
        Err(DescribeShareGroupProtocolFailure::DuplicatePartition)
    );

    response.groups[0].members[0].assignment.topic_partitions[0].partitions = vec![0];
    assert!(matches!(
        normalize_describe_share_group_response("share-readers", false, Some(1), &response, 1,),
        Err(DescribeShareGroupProtocolFailure::RetainedBytes { .. })
    ));
}

#[test]
fn duplicate_members_topics_and_unrequested_authorization_fail_closed() {
    let mut response = ShareGroupDescribeResponse::default();
    let mut group = success_group();
    group.members = vec![
        member("same", vec![], vec![]),
        member("same", vec![], vec![]),
    ];
    response.groups = vec![group];
    assert_eq!(
        normalize_describe_share_group_response(
            "share-readers",
            false,
            Some(1),
            &response,
            RESULT_LIMIT,
        ),
        Err(DescribeShareGroupProtocolFailure::DuplicateMemberId)
    );

    response.groups[0].members = vec![member(
        "member",
        vec![],
        vec![
            assigned_topic("orders", [1; 16], vec![0]),
            assigned_topic("orders", [2; 16], vec![1]),
        ],
    )];
    assert_eq!(
        normalize_describe_share_group_response(
            "share-readers",
            false,
            Some(1),
            &response,
            RESULT_LIMIT,
        ),
        Err(DescribeShareGroupProtocolFailure::DuplicateTopicName)
    );

    response.groups[0].members = Vec::new();
    response.groups[0].authorized_operations = 3;
    assert_eq!(
        normalize_describe_share_group_response(
            "share-readers",
            false,
            Some(1),
            &response,
            RESULT_LIMIT,
        ),
        Err(DescribeShareGroupProtocolFailure::UnexpectedAuthorizedOperations)
    );
}

fn success_group() -> DescribedGroup {
    let mut group = DescribedGroup::default();
    group.group_id = "share-readers".into();
    group.group_state = "Stable".into();
    group.group_epoch = 8;
    group.assignment_epoch = 7;
    group.assignor_name = "share".into();
    group.authorized_operations = i32::MIN;
    group
}

fn member(member_id: &str, subscriptions: Vec<&str>, topics: Vec<TopicPartitions>) -> Member {
    let mut member = Member::default();
    member.member_id = member_id.into();
    member.member_epoch = 4;
    member.client_id = "client".into();
    member.client_host = "host".into();
    member.subscribed_topic_names = subscriptions.into_iter().map(StrBytes::from).collect();
    let mut assignment = Assignment::default();
    assignment.topic_partitions = topics;
    member.assignment = assignment;
    member
}

fn assigned_topic(name: &str, id: [u8; 16], partitions: Vec<i32>) -> TopicPartitions {
    let mut topic = TopicPartitions::default();
    topic.topic_name = name.into();
    topic.topic_id = Uuid::from_bytes(id);
    topic.partitions = partitions;
    topic
}
