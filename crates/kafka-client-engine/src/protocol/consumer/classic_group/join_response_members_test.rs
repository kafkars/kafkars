//! Leader-member bounds, subscription validation, and owned spelling scenarios.

use std::sync::Arc;

use kafka_client_core::{ClassicGeneration, ClassicProtocol};
use kafka_wire::{
    ConsumerProtocolSubscription, consumer_protocol_subscription::TopicPartition,
    encode_consumer_protocol_subscription,
};
use kafka_wire_core::{ApiVersion, BytesMut};

use super::{
    ClassicJoinOutcome, ClassicJoinResponseFailure,
    join_response_test_fixture::{member, metadata, response},
    normalize_classic_join_response as normalize_classic_join_response_for_protocol,
    validation::{COOPERATIVE_STICKY_PROTOCOL, MAX_MEMBER_PARTITIONS, MAX_MEMBERS},
};

fn normalize_classic_join_response(
    selected_version: i16,
    response: &kafka_wire::JoinGroupResponse,
) -> Result<ClassicJoinOutcome, ClassicJoinResponseFailure> {
    normalize_classic_join_response_for_protocol(selected_version, ClassicProtocol::Range, response)
}

#[test]
fn leader_members_are_bounded_dynamic_and_uniquely_correlated() {
    let mut raw = response("a", "a");
    raw.members = vec![member("b", &["orders"])];
    assert_eq!(
        normalize_classic_join_response(3, &raw),
        Err(ClassicJoinResponseFailure::LeaderMemberMissing)
    );
    raw.members = vec![member("a", &["orders"]), member("a", &["orders"])];
    assert_eq!(
        normalize_classic_join_response(3, &raw),
        Err(ClassicJoinResponseFailure::DuplicateMember)
    );
    raw.members = (0..=MAX_MEMBERS)
        .map(|index| member(&format!("member-{index}"), &[]))
        .collect();
    assert_eq!(
        normalize_classic_join_response(3, &raw),
        Err(ClassicJoinResponseFailure::MemberCount {
            actual: MAX_MEMBERS + 1,
            limit: MAX_MEMBERS,
        })
    );
    raw.members = vec![member("a", &["orders"])];
    raw.members[0].group_instance_id = Some("static-a".into());
    assert_eq!(
        normalize_classic_join_response(3, &raw),
        Err(ClassicJoinResponseFailure::StaticMember)
    );
}

#[test]
fn v5_leader_accepts_bounded_static_member_identities() {
    let mut raw = response("a", "a");
    raw.members = vec![member("a", &["orders"])];
    raw.members[0].group_instance_id = Some("instance-a".into());
    assert!(matches!(
        normalize_classic_join_response(5, &raw),
        Ok(ClassicJoinOutcome::Joined(_))
    ));

    raw.members[0].group_instance_id = Some("".into());
    assert_eq!(
        normalize_classic_join_response(5, &raw),
        Err(ClassicJoinResponseFailure::StaticMember)
    );
}

#[test]
fn subscription_version_and_duplicate_topics_are_rejected() {
    let mut raw = response("a", "a");
    raw.members = vec![member("a", &["orders"])];
    raw.members[0].metadata = metadata(1, &["orders"]);
    assert_eq!(
        normalize_classic_join_response(3, &raw),
        Err(ClassicJoinResponseFailure::UnsupportedSubscriptionVersion(
            1
        ))
    );
    raw.members[0].metadata = metadata(0, &["orders", "orders"]);
    assert_eq!(
        normalize_classic_join_response(3, &raw),
        Err(ClassicJoinResponseFailure::DuplicateTopic)
    );
    let mut subscription = ConsumerProtocolSubscription::default();
    subscription.topics.push("orders".into());
    subscription.user_data = Some(kafka_wire_core::Bytes::from_static(b"opaque"));
    let mut encoded = BytesMut::new();
    encode_consumer_protocol_subscription(&mut encoded, &subscription, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("subscription encode failed: {error}"));
    raw.members[0].metadata = encoded.freeze();
    assert_eq!(
        normalize_classic_join_response(3, &raw),
        Err(ClassicJoinResponseFailure::SubscriptionUserData)
    );
}

#[test]
fn normalized_member_and_topic_spellings_are_arc_owned() {
    let mut raw = response("local", "local");
    raw.members = vec![member("local", &["orders"])];
    let ClassicJoinOutcome::Joined(joined) = normalize_classic_join_response(3, &raw)
        .unwrap_or_else(|error| panic!("Join normalization failed: {error:?}"))
    else {
        panic!("joined outcome expected");
    };
    let (_, _, local, role) = joined.into_parts();
    let (_, member, topics, owned, generation) = role
        .into_leader_members()
        .unwrap_or_else(|| panic!("leader members"))
        .remove(0)
        .into_parts();
    assert_eq!(local, Arc::<str>::from("local"));
    assert_eq!(member, Arc::<str>::from("local"));
    assert_eq!(topics, vec![Arc::<str>::from("orders")]);
    assert!(owned.is_empty());
    assert_eq!(generation, None);
}

#[test]
fn cooperative_v1_through_v3_members_retain_dropped_topic_ownership() {
    for (version, rack_id) in [(1, None), (2, None), (3, None), (3, Some("rack-a"))] {
        let mut raw = response("local", "local");
        raw.protocol_name = Some(COOPERATIVE_STICKY_PROTOCOL.into());
        let mut subscription = ConsumerProtocolSubscription::default();
        subscription.topics = vec!["payments".into()];
        let mut orders = TopicPartition::default();
        orders.topic = "orders".into();
        orders.partitions = vec![1, 3];
        let mut payments = TopicPartition::default();
        payments.topic = "payments".into();
        payments.partitions = vec![0];
        subscription.owned_partitions = vec![orders, payments];
        if version >= 2 {
            subscription.generation_id = 6;
        }
        subscription.rack_id = rack_id.map(Into::into);
        let mut encoded = BytesMut::new();
        encode_consumer_protocol_subscription(
            &mut encoded,
            &subscription,
            ApiVersion::new(version),
        )
        .unwrap_or_else(|error| panic!("subscription encode failed: {error}"));
        let mut joined_member = member("local", &[]);
        joined_member.metadata = encoded.freeze();
        raw.members = vec![joined_member];
        let ClassicJoinOutcome::Joined(joined) = normalize_classic_join_response_for_protocol(
            3,
            ClassicProtocol::CooperativeSticky,
            &raw,
        )
        .unwrap_or_else(|error| panic!("cooperative normalization failed: {error:?}")) else {
            panic!("joined outcome expected");
        };
        let (_, _, _, role) = joined.into_parts();
        let (_, _, topics, owned, generation) = role
            .into_leader_members()
            .unwrap_or_else(|| panic!("leader members"))
            .remove(0)
            .into_parts();
        assert_eq!(topics, vec![Arc::<str>::from("payments")]);
        assert_eq!(owned.len(), 3);
        assert_eq!(owned[0].topic(), "orders");
        assert_eq!(owned[0].partition(), 1);
        assert_eq!(owned[2].topic(), "payments");
        assert_eq!(owned[2].partition(), 0);
        assert_eq!(
            generation,
            if version >= 2 {
                ClassicGeneration::try_from_raw(6)
            } else {
                None
            }
        );
    }
}

#[test]
fn cooperative_owned_facts_are_strictly_ordered_and_bounded() {
    let mut raw = response("local", "local");
    raw.protocol_name = Some(COOPERATIVE_STICKY_PROTOCOL.into());
    let cases = [
        (
            vec![("orders", vec![1, 1])],
            ClassicJoinResponseFailure::DuplicateOwnedPartition,
        ),
        (
            vec![("orders", vec![2, 1])],
            ClassicJoinResponseFailure::OutOfOrderOwnedPartition,
        ),
        (
            vec![("orders", vec![-1])],
            ClassicJoinResponseFailure::InvalidOwnedPartition(-1),
        ),
        (
            vec![("payments", vec![0]), ("orders", vec![0])],
            ClassicJoinResponseFailure::OutOfOrderOwnedTopic,
        ),
    ];
    for (owned, expected) in cases {
        raw.members = vec![cooperative_member("local", &owned, 2, 4, None)];
        assert_eq!(
            normalize_classic_join_response_for_protocol(
                3,
                ClassicProtocol::CooperativeSticky,
                &raw,
            ),
            Err(expected)
        );
    }
    let maximum_partition = i32::try_from(MAX_MEMBER_PARTITIONS)
        .unwrap_or_else(|error| panic!("member partition limit fits i32: {error:?}"));
    raw.members = vec![cooperative_member(
        "local",
        &[("orders", (0..=maximum_partition).collect())],
        2,
        4,
        None,
    )];
    assert_eq!(
        normalize_classic_join_response_for_protocol(3, ClassicProtocol::CooperativeSticky, &raw,),
        Err(ClassicJoinResponseFailure::OwnedPartitionCount {
            actual: MAX_MEMBER_PARTITIONS + 1,
            limit: MAX_MEMBER_PARTITIONS,
        })
    );
}

#[test]
fn range_and_cooperative_subscription_versions_do_not_cross() {
    let mut raw = response("local", "local");
    raw.members = vec![member("local", &["orders"])];
    assert_eq!(
        normalize_classic_join_response_for_protocol(3, ClassicProtocol::CooperativeSticky, &raw,),
        Err(ClassicJoinResponseFailure::UnexpectedProtocolName)
    );
    raw.protocol_name = Some(COOPERATIVE_STICKY_PROTOCOL.into());
    assert_eq!(
        normalize_classic_join_response_for_protocol(3, ClassicProtocol::CooperativeSticky, &raw,),
        Err(ClassicJoinResponseFailure::UnsupportedSubscriptionVersion(
            0
        ))
    );
    raw.members = vec![cooperative_member(
        "local",
        &[("orders", vec![0])],
        2,
        -2,
        None,
    )];
    assert_eq!(
        normalize_classic_join_response_for_protocol(3, ClassicProtocol::CooperativeSticky, &raw,),
        Err(ClassicJoinResponseFailure::InvalidSubscriptionGeneration(
            -2
        ))
    );
    raw.members = vec![cooperative_member(
        "local",
        &[("orders", vec![0])],
        3,
        4,
        Some(""),
    )];
    assert_eq!(
        normalize_classic_join_response_for_protocol(3, ClassicProtocol::CooperativeSticky, &raw,),
        Err(ClassicJoinResponseFailure::SubscriptionRackId)
    );
}

fn cooperative_member(
    name: &str,
    owned: &[(&str, Vec<i32>)],
    version: i16,
    generation: i32,
    rack_id: Option<&str>,
) -> kafka_wire::join_group_response::JoinGroupResponseMember {
    let mut subscription = ConsumerProtocolSubscription::default();
    subscription.topics = ["orders", "payments"].into_iter().map(Into::into).collect();
    subscription.owned_partitions = owned
        .iter()
        .map(|(topic, partitions)| {
            let mut owned = TopicPartition::default();
            owned.topic = (*topic).into();
            owned.partitions = partitions.clone();
            owned
        })
        .collect();
    subscription.generation_id = generation;
    subscription.rack_id = rack_id.map(Into::into);
    let mut encoded = BytesMut::new();
    encode_consumer_protocol_subscription(&mut encoded, &subscription, ApiVersion::new(version))
        .unwrap_or_else(|error| panic!("subscription encode failed: {error}"));
    let mut member = member(name, &[]);
    member.metadata = encoded.freeze();
    member
}
