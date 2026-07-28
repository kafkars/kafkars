//! Leader-member bounds, subscription validation, and owned spelling scenarios.

use std::sync::Arc;

use kafka_wire::{ConsumerProtocolSubscription, encode_consumer_protocol_subscription};
use kafka_wire_core::{ApiVersion, BytesMut};

use super::{
    ClassicJoinOutcome, ClassicJoinResponseFailure,
    join_response_test_fixture::{member, metadata, response},
    normalize_classic_join_response,
    validation::MAX_MEMBERS,
};

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
    let (_, member, topics) = role
        .into_leader_members()
        .unwrap_or_else(|| panic!("leader members"))
        .remove(0)
        .into_parts();
    assert_eq!(local, Arc::<str>::from("local"));
    assert_eq!(member, Arc::<str>::from("local"));
    assert_eq!(topics, vec![Arc::<str>::from("orders")]);
}
