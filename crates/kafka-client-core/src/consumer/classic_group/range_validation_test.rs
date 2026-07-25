//! Direct classic Range count-validation evidence.

use crate::{MemberId, TopicId};

use super::{
    ClassicAssignmentError, ClassicJoinMember, ClassicJoinMembers, ClassicSubscription,
    JoinedMemberSlot, MemberRank, TopicPartitionCount, range_validation::validate_counts,
};

#[test]
fn counts_cover_every_subscribed_topic_in_order() {
    let members = joined();
    assert_eq!(
        validate_counts(&members, &[count(1, 2), count(2, 3)]),
        Ok(())
    );
    assert_eq!(
        validate_counts(&members, &[count(1, 2)]),
        Err(ClassicAssignmentError::MissingTopicCount(topic(2)))
    );
    assert_eq!(
        validate_counts(&members, &[count(2, 3), count(1, 2)]),
        Err(ClassicAssignmentError::OutOfOrderTopicCount)
    );
}

#[test]
fn counts_reject_duplicate_unsubscribed_and_signed_overflow() {
    let members = joined();
    assert_eq!(
        validate_counts(&members, &[count(1, 1), count(1, 2)]),
        Err(ClassicAssignmentError::DuplicateTopicCount(topic(1)))
    );
    assert_eq!(
        validate_counts(&members, &[count(1, 1), count(2, 1), count(3, 1)]),
        Err(ClassicAssignmentError::UnsubscribedTopicCount(topic(3)))
    );
    assert_eq!(
        validate_counts(&members, &[count(1, i32::MAX as u32 + 1), count(2, 1)]),
        Err(ClassicAssignmentError::PartitionCountOutOfRange(topic(1)))
    );
}

fn joined() -> ClassicJoinMembers {
    let subscription = ClassicSubscription::try_new(vec![topic(1), topic(2)])
        .unwrap_or_else(|error| panic!("valid subscription: {error:?}"));
    let member = ClassicJoinMember::new(slot(1), member(1), rank(1), subscription);
    ClassicJoinMembers::try_new(vec![member])
        .unwrap_or_else(|error| panic!("valid members: {error:?}"))
}

fn count(topic_id: u64, partitions: u32) -> TopicPartitionCount {
    TopicPartitionCount::new(topic(topic_id), partitions)
}

fn slot(value: u32) -> JoinedMemberSlot {
    JoinedMemberSlot::try_from_raw(value).unwrap_or_else(|| panic!("nonzero slot"))
}

fn rank(value: u32) -> MemberRank {
    MemberRank::try_from_raw(value).unwrap_or_else(|| panic!("nonzero rank"))
}

fn member(value: u64) -> MemberId {
    MemberId::try_from_raw(value).unwrap_or_else(|| panic!("nonzero member"))
}

const fn topic(value: u64) -> TopicId {
    TopicId::from_raw(value)
}
