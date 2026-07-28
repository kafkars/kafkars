//! KIP-848 response preservation, bounds, correlation, and fallback scenarios.

use kafka_wire::{
    ConsumerGroupDescribeResponse,
    consumer_group_describe_response::{Assignment, DescribedGroup, Member, TopicPartitions},
};
use kafka_wire_core::{StrBytes, TaggedFields, Uuid};

use super::{
    ConsumerGroupDescribeFallback, ConsumerGroupDescribeResponseFailure,
    ConsumerGroupDescribeResult, normalize_consumer_group_describe_response,
};

#[test]
fn v1_success_preserves_modern_group_member_and_assignment_facts() {
    let mut current = topic([1; 16], "orders", vec![2, 0]);
    current.unknown_tagged_fields = TaggedFields::default();
    let target = topic([2; 16], "payments", vec![7]);
    let mut member = Member::default();
    member.member_id = "member-a".into();
    member.instance_id = Some("instance-a".into());
    member.rack_id = Some("rack-a".into());
    member.member_epoch = 8;
    member.client_id = "client-a".into();
    member.client_host = "host-a".into();
    member.subscribed_topic_names = vec!["payments".into(), "orders".into()];
    member.subscribed_topic_regex = Some("orders-.*".into());
    member.assignment = assignment(vec![current]);
    member.target_assignment = assignment(vec![target]);
    member.member_type = 1;
    let mut group = described_group("workers");
    group.group_state = "Stable".into();
    group.group_epoch = 11;
    group.assignment_epoch = 9;
    group.assignor_name = "uniform".into();
    group.members = vec![member];
    group.authorized_operations = 0x42;

    let normalized = normalize_consumer_group_describe_response(
        "workers",
        true,
        1,
        &response(group),
        4 * 1024 * 1024,
    )
    .unwrap_or_else(|error| panic!("valid modern response: {error:?}"));
    let (throttle, group_id, result, fallback, retained) = normalized.into_parts();
    assert_eq!(throttle, 12);
    assert_eq!(group_id, "workers");
    assert_eq!(fallback, None);
    assert!(retained > 0);
    let ConsumerGroupDescribeResult::Described(description) = result else {
        panic!("successful group became broker error");
    };
    let (state, group_epoch, assignment_epoch, assignor, members, operations) =
        description.into_parts();
    assert_eq!(state, "Stable");
    assert_eq!(group_epoch, 11);
    assert_eq!(assignment_epoch, 9);
    assert_eq!(assignor, "uniform");
    assert_eq!(operations, Some(0x42));
    let (
        member_id,
        instance_id,
        rack_id,
        member_epoch,
        client_id,
        client_host,
        subscriptions,
        regex,
        current,
        target,
        member_type,
    ) = members
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("expected one member"))
        .into_parts();
    assert_eq!(member_id, "member-a");
    assert_eq!(instance_id.as_deref(), Some("instance-a"));
    assert_eq!(rack_id.as_deref(), Some("rack-a"));
    assert_eq!(member_epoch, 8);
    assert_eq!(client_id, "client-a");
    assert_eq!(client_host, "host-a");
    assert_eq!(subscriptions, ["orders", "payments"]);
    assert_eq!(regex.as_deref(), Some("orders-.*"));
    assert_eq!(member_type, Some(1));
    let (topic_id, topic_name, partitions) = current
        .into_topics()
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("expected current topic"))
        .into_parts();
    assert_eq!(topic_id, [1; 16]);
    assert_eq!(topic_name, "orders");
    assert_eq!(partitions, [0, 2]);
    assert_eq!(target.topics()[0].topic_id(), &[2; 16]);
}

#[test]
fn v0_retains_unknown_member_type_as_absence() {
    let mut member = Member::default();
    member.member_id = "member-a".into();
    member.member_type = -1;
    let mut group = described_group("workers");
    group.members = vec![member];
    let normalized = normalize_consumer_group_describe_response(
        "workers",
        false,
        0,
        &response(group),
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("valid v0 response: {error:?}"));
    let (_, _, ConsumerGroupDescribeResult::Described(description), _, _) = normalized.into_parts()
    else {
        panic!("expected description");
    };
    let (_, _, _, _, members, _) = description.into_parts();
    assert_eq!(
        members
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("expected member"))
            .into_parts()
            .10,
        None
    );
}

#[test]
fn exact_broker_errors_keep_diagnostics_and_fallback_signals() {
    for (code, expected_fallback) in [
        (
            35,
            Some(ConsumerGroupDescribeFallback::BrokerUnsupportedVersion),
        ),
        (
            69,
            Some(ConsumerGroupDescribeFallback::BrokerGroupIdNotFound),
        ),
        (15, None),
    ] {
        let mut group = described_group("workers");
        group.error_code = code;
        group.error_message = Some("x".repeat(1025).into());
        let normalized = normalize_consumer_group_describe_response(
            "workers",
            false,
            1,
            &response(group),
            usize::MAX,
        )
        .unwrap_or_else(|error| panic!("valid broker rejection: {error:?}"));
        let (_, _, ConsumerGroupDescribeResult::Failed(error), fallback, _) =
            normalized.into_parts()
        else {
            panic!("expected exact broker error");
        };
        let (actual_code, message, truncated) = error.into_parts();
        assert_eq!(actual_code, code);
        assert_eq!(
            message
                .unwrap_or_else(|| panic!("expected diagnostic"))
                .len(),
            1024
        );
        assert!(truncated);
        assert_eq!(fallback, expected_fallback);
    }
}

#[test]
fn correlation_assignment_shape_and_capacity_are_strict() {
    assert_eq!(
        normalize_consumer_group_describe_response(
            "workers",
            false,
            2,
            &response(described_group("workers")),
            usize::MAX,
        ),
        Err(ConsumerGroupDescribeResponseFailure::LocalUnsupportedVersion { actual: 2 })
    );
    assert_eq!(
        normalize_consumer_group_describe_response(
            "workers",
            false,
            1,
            &ConsumerGroupDescribeResponse::default(),
            usize::MAX,
        ),
        Err(ConsumerGroupDescribeResponseFailure::MissingGroup)
    );
    assert_eq!(
        normalize_consumer_group_describe_response(
            "workers",
            false,
            1,
            &response(described_group("other")),
            usize::MAX,
        ),
        Err(ConsumerGroupDescribeResponseFailure::UnexpectedGroup)
    );
    let mut invalid_partition = Member::default();
    invalid_partition.member_id = "member".into();
    invalid_partition.assignment = assignment(vec![topic([1; 16], "orders", vec![-1])]);
    let mut group = described_group("workers");
    group.members = vec![invalid_partition];
    assert_eq!(
        normalize_consumer_group_describe_response(
            "workers",
            false,
            1,
            &response(group),
            usize::MAX,
        ),
        Err(ConsumerGroupDescribeResponseFailure::Partition)
    );
    assert_eq!(
        normalize_consumer_group_describe_response(
            "workers",
            false,
            1,
            &response(described_group("workers")),
            1,
        ),
        Err(ConsumerGroupDescribeResponseFailure::ResponseTooLarge)
    );
}

#[test]
fn signed_epochs_are_preserved_without_inventing_sentinels() {
    let mut member = Member::default();
    member.member_id = "member".into();
    member.member_epoch = -3;
    let mut group = described_group("workers");
    group.group_epoch = -1;
    group.assignment_epoch = -2;
    group.members = vec![member];
    let normalized = normalize_consumer_group_describe_response(
        "workers",
        false,
        1,
        &response(group),
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("signed epochs are wire-valid: {error:?}"));
    let (_, _, ConsumerGroupDescribeResult::Described(description), _, _) = normalized.into_parts()
    else {
        panic!("expected description");
    };
    let (_, group_epoch, assignment_epoch, _, members, _) = description.into_parts();
    assert_eq!(group_epoch, -1);
    assert_eq!(assignment_epoch, -2);
    assert_eq!(
        members
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("expected member"))
            .into_parts()
            .3,
        -3
    );
}

#[test]
fn duplicate_subscriptions_topics_and_partitions_are_rejected() {
    let mut duplicate_subscription = Member::default();
    duplicate_subscription.member_id = "member".into();
    duplicate_subscription.subscribed_topic_names = vec!["orders".into(), "orders".into()];
    let mut group = described_group("workers");
    group.members = vec![duplicate_subscription];
    assert_eq!(
        normalize_consumer_group_describe_response(
            "workers",
            false,
            1,
            &response(group),
            usize::MAX,
        ),
        Err(ConsumerGroupDescribeResponseFailure::DuplicateSubscription)
    );

    let mut duplicate_partitions = Member::default();
    duplicate_partitions.member_id = "member".into();
    duplicate_partitions.assignment = assignment(vec![topic([1; 16], "orders", vec![2, 2])]);
    let mut group = described_group("workers");
    group.members = vec![duplicate_partitions];
    assert_eq!(
        normalize_consumer_group_describe_response(
            "workers",
            false,
            1,
            &response(group),
            usize::MAX,
        ),
        Err(ConsumerGroupDescribeResponseFailure::DuplicatePartition)
    );

    let mut duplicate_topics = Member::default();
    duplicate_topics.member_id = "member".into();
    duplicate_topics.assignment = assignment(vec![
        topic([1; 16], "orders", vec![0]),
        topic([1; 16], "other-name", vec![1]),
    ]);
    let mut group = described_group("workers");
    group.members = vec![duplicate_topics];
    assert_eq!(
        normalize_consumer_group_describe_response(
            "workers",
            false,
            1,
            &response(group),
            usize::MAX,
        ),
        Err(ConsumerGroupDescribeResponseFailure::DuplicateTopicId)
    );
}

#[test]
fn subscription_owner_and_sort_scratch_are_in_the_retained_charge() {
    let mut without = Member::default();
    without.member_id = "member".into();
    let mut group_without = described_group("workers");
    group_without.members = vec![without];
    let (_, _, _, _, base) = normalize_consumer_group_describe_response(
        "workers",
        false,
        1,
        &response(group_without),
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("valid base response: {error:?}"))
    .into_parts();

    let mut with = Member::default();
    with.member_id = "member".into();
    with.subscribed_topic_names = vec!["orders".into()];
    let mut group_with = described_group("workers");
    group_with.members = vec![with];
    let (_, _, _, _, charged) = normalize_consumer_group_describe_response(
        "workers",
        false,
        1,
        &response(group_with),
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("valid response with subscription: {error:?}"))
    .into_parts();
    assert!(
        charged - base
            >= "orders".len() + core::mem::size_of::<String>() + core::mem::size_of::<&StrBytes>()
    );
}

fn described_group(group_id: &str) -> DescribedGroup {
    let mut group = DescribedGroup::default();
    group.group_id = group_id.into();
    group
}

fn response(group: DescribedGroup) -> ConsumerGroupDescribeResponse {
    let mut response = ConsumerGroupDescribeResponse::default();
    response.throttle_time_ms = 12;
    response.groups = vec![group];
    response
}

fn topic(topic_id: [u8; 16], topic_name: &str, partitions: Vec<i32>) -> TopicPartitions {
    let mut topic = TopicPartitions::default();
    topic.topic_id = Uuid::from_bytes(topic_id);
    topic.topic_name = topic_name.into();
    topic.partitions = partitions;
    topic
}

fn assignment(topics: Vec<TopicPartitions>) -> Assignment {
    let mut assignment = Assignment::default();
    assignment.topic_partitions = topics;
    assignment
}
