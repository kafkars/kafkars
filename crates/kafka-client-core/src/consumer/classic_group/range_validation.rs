//! Structural validation of scalar topic counts before Range planning.

use crate::TopicId;

use super::{ClassicAssignmentError, ClassicJoinMembers, TopicPartitionCount};

pub(super) fn validate_counts(
    members: &ClassicJoinMembers,
    counts: &[TopicPartitionCount],
) -> Result<(), ClassicAssignmentError> {
    for pair in counts.windows(2) {
        if pair[0].topic_id() == pair[1].topic_id() {
            return Err(ClassicAssignmentError::DuplicateTopicCount(
                pair[0].topic_id(),
            ));
        }
        if pair[0].topic_id() > pair[1].topic_id() {
            return Err(ClassicAssignmentError::OutOfOrderTopicCount);
        }
    }
    for count in counts {
        if count.count() > i32::MAX as u32 {
            return Err(ClassicAssignmentError::PartitionCountOutOfRange(
                count.topic_id(),
            ));
        }
        if !subscribed(members, count.topic_id()) {
            return Err(ClassicAssignmentError::UnsubscribedTopicCount(
                count.topic_id(),
            ));
        }
    }
    for member in members.members() {
        for topic in member.subscription().topics() {
            if !counts.iter().any(|count| count.topic_id() == *topic) {
                return Err(ClassicAssignmentError::MissingTopicCount(*topic));
            }
        }
    }
    Ok(())
}

fn subscribed(members: &ClassicJoinMembers, topic_id: TopicId) -> bool {
    members
        .members()
        .iter()
        .any(|member| member.subscription().topics().contains(&topic_id))
}
