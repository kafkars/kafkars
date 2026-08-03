//! Stable owned classic-group fact extraction scenarios.

use std::sync::Arc;

use kafka_client_core::{JoinedMemberSlot, TopicId};

use super::{ClassicJoinedMember, ClassicSyncMember, ClassicSyncTopic, NamedAssignmentPartition};

#[test]
fn candidate_and_assignment_parts_move_arc_spellings_without_copying() {
    let slot = JoinedMemberSlot::try_from_raw(1).unwrap_or_else(|| panic!("slot"));
    let member_name: Arc<str> = Arc::from("member-a");
    let topic_name: Arc<str> = Arc::from("orders");
    let member = ClassicJoinedMember::new(
        slot,
        Arc::clone(&member_name),
        vec![Arc::clone(&topic_name)],
        Vec::new(),
        None,
    );
    let (_, moved_member, moved_topics, moved_owned, moved_generation) = member.into_parts();
    assert!(Arc::ptr_eq(&member_name, &moved_member));
    assert!(Arc::ptr_eq(&topic_name, &moved_topics[0]));
    assert!(moved_owned.is_empty());
    assert_eq!(moved_generation, None);

    let mapped_member = ClassicSyncMember::new(slot, Arc::clone(&member_name));
    assert_eq!(mapped_member.slot(), slot);
    assert_eq!(mapped_member.member(), "member-a");
    let mapped_topic = ClassicSyncTopic::new(TopicId::from_raw(1), Arc::clone(&topic_name));
    assert_eq!(mapped_topic.topic_id(), TopicId::from_raw(1));
    assert_eq!(mapped_topic.topic(), "orders");

    let partition = NamedAssignmentPartition::new(Arc::clone(&topic_name), 4);
    let (moved_topic, index) = partition.into_parts();
    assert!(Arc::ptr_eq(&topic_name, &moved_topic));
    assert_eq!(index, 4);
}
