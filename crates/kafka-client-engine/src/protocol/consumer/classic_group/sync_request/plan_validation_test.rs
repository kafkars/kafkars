//! Member-slot and topic-correlation rejection scenarios.

use std::sync::Arc;

use kafka_client_core::{JoinedMemberSlot, TopicId};

use super::{
    ClassicSyncMember, ClassicSyncRequestFailure, ClassicSyncTopic, member_for_slot,
    validate_members, validate_topics,
};

fn slot(raw: u32) -> JoinedMemberSlot {
    JoinedMemberSlot::try_from_raw(raw).unwrap_or_else(|| panic!("slot"))
}

#[test]
fn empty_plan_rejects_an_unowned_member() {
    let members = [ClassicSyncMember::new(slot(1), Arc::from("member-a"))];
    assert_eq!(
        validate_members(&[], &members, "member-a"),
        Err(ClassicSyncRequestFailure::UnexpectedMember(slot(1)))
    );
}

#[test]
fn duplicate_topic_identity_is_rejected_before_materialization() {
    let topics = [
        ClassicSyncTopic::new(TopicId::from_raw(1), Arc::from("orders")),
        ClassicSyncTopic::new(TopicId::from_raw(1), Arc::from("payments")),
    ];
    assert_eq!(
        validate_topics(&topics),
        Err(ClassicSyncRequestFailure::DuplicateTopicId(
            TopicId::from_raw(1)
        ))
    );
}

#[test]
fn member_lookup_preserves_the_exact_slot_mapping() {
    let members = [ClassicSyncMember::new(slot(2), Arc::from("member-b"))];
    assert_eq!(member_for_slot(&members, slot(2)), Ok("member-b"));
    assert_eq!(
        member_for_slot(&members, slot(1)),
        Err(ClassicSyncRequestFailure::MissingMember(slot(1)))
    );
}
