//! Borrowed validation before any linear commit owner is consumed.

use kafka_client_core::GroupOffsetCommitEffect;

use crate::clock::OperationDeadline;

use super::{
    preparation::GroupOffsetCommitPreparationErrorKind,
    session::{
        ClassicGroupCommitSession, GroupOffsetCommitTopicName, MAX_GROUP_OFFSET_COMMIT_ID_BYTES,
        MAX_GROUP_OFFSET_COMMIT_TOPIC_BYTES,
    },
};

/// Maximum topic-partition entries retained by one commit snapshot.
pub(super) const MAX_GROUP_OFFSET_COMMIT_ENTRIES: usize = 64;

pub(super) fn validate_group_offset_commit_inputs(
    effect: &GroupOffsetCommitEffect,
    operation_deadline: OperationDeadline,
    session: &ClassicGroupCommitSession,
    topic_names: &[GroupOffsetCommitTopicName],
) -> Result<usize, GroupOffsetCommitPreparationErrorKind> {
    let GroupOffsetCommitEffect::Submit {
        deadline,
        checkpoint,
        ..
    } = effect
    else {
        return Err(GroupOffsetCommitPreparationErrorKind::UnexpectedEffect);
    };
    let entry_count = checkpoint.entries().len();
    if entry_count > MAX_GROUP_OFFSET_COMMIT_ENTRIES {
        return Err(GroupOffsetCommitPreparationErrorKind::EntryCapacity {
            actual: entry_count,
            limit: MAX_GROUP_OFFSET_COMMIT_ENTRIES,
        });
    }
    if topic_names.len() > MAX_GROUP_OFFSET_COMMIT_ENTRIES {
        return Err(GroupOffsetCommitPreparationErrorKind::TopicCapacity {
            actual: topic_names.len(),
            limit: MAX_GROUP_OFFSET_COMMIT_ENTRIES,
        });
    }
    if *deadline != operation_deadline.core() {
        return Err(GroupOffsetCommitPreparationErrorKind::DeadlineMismatch {
            effect: *deadline,
            operation: operation_deadline.core(),
        });
    }
    if checkpoint.group_id() != session.group_id {
        return Err(GroupOffsetCommitPreparationErrorKind::GroupMismatch);
    }
    if checkpoint.member_id() != session.member_id {
        return Err(GroupOffsetCommitPreparationErrorKind::MemberMismatch);
    }
    if checkpoint.assignment_generation() != session.assignment_generation {
        return Err(GroupOffsetCommitPreparationErrorKind::GenerationMismatch);
    }
    validate_session(session)?;
    validate_topic_names(topic_names)?;
    for entry in checkpoint.entries() {
        if i32::try_from(entry.partition().get()).is_err() {
            return Err(GroupOffsetCommitPreparationErrorKind::PartitionOutOfRange {
                topic_id: entry.topic_id(),
                partition: entry.partition(),
            });
        }
        if !topic_names
            .iter()
            .any(|topic| topic.topic_id == entry.topic_id())
        {
            return Err(GroupOffsetCommitPreparationErrorKind::UnknownTopic(
                entry.topic_id(),
            ));
        }
    }
    if topic_names.iter().any(|topic| {
        !checkpoint
            .entries()
            .iter()
            .any(|entry| entry.topic_id() == topic.topic_id)
    }) {
        return Err(GroupOffsetCommitPreparationErrorKind::UnusedTopic);
    }
    Ok(entry_count)
}

fn validate_session(
    session: &ClassicGroupCommitSession,
) -> Result<(), GroupOffsetCommitPreparationErrorKind> {
    validate_id(
        &session.group,
        GroupOffsetCommitPreparationErrorKind::EmptyGroup,
        |actual| GroupOffsetCommitPreparationErrorKind::GroupTooLong {
            actual,
            limit: MAX_GROUP_OFFSET_COMMIT_ID_BYTES,
        },
    )?;
    validate_id(
        &session.member,
        GroupOffsetCommitPreparationErrorKind::EmptyMember,
        |actual| GroupOffsetCommitPreparationErrorKind::MemberTooLong {
            actual,
            limit: MAX_GROUP_OFFSET_COMMIT_ID_BYTES,
        },
    )?;
    if let Some(group_instance_id) = &session.group_instance_id {
        validate_id(
            group_instance_id,
            GroupOffsetCommitPreparationErrorKind::EmptyGroupInstance,
            |actual| GroupOffsetCommitPreparationErrorKind::GroupInstanceTooLong {
                actual,
                limit: MAX_GROUP_OFFSET_COMMIT_ID_BYTES,
            },
        )?;
    }
    let Ok(classic_generation) = i32::try_from(session.classic_generation) else {
        return Err(GroupOffsetCommitPreparationErrorKind::ClassicGenerationOutOfRange);
    };
    if classic_generation < 0 {
        return Err(GroupOffsetCommitPreparationErrorKind::ClassicGenerationOutOfRange);
    }
    Ok(())
}

fn validate_id(
    value: &str,
    empty: GroupOffsetCommitPreparationErrorKind,
    too_long: impl FnOnce(usize) -> GroupOffsetCommitPreparationErrorKind,
) -> Result<(), GroupOffsetCommitPreparationErrorKind> {
    if value.is_empty() {
        return Err(empty);
    }
    if value.len() > MAX_GROUP_OFFSET_COMMIT_ID_BYTES {
        return Err(too_long(value.len()));
    }
    Ok(())
}

fn validate_topic_names(
    topic_names: &[GroupOffsetCommitTopicName],
) -> Result<(), GroupOffsetCommitPreparationErrorKind> {
    for (index, topic) in topic_names.iter().enumerate() {
        if topic.name.is_empty() {
            return Err(GroupOffsetCommitPreparationErrorKind::EmptyTopicName);
        }
        if topic.name.len() > MAX_GROUP_OFFSET_COMMIT_TOPIC_BYTES {
            return Err(GroupOffsetCommitPreparationErrorKind::TopicNameTooLong {
                actual: topic.name.len(),
                limit: MAX_GROUP_OFFSET_COMMIT_TOPIC_BYTES,
            });
        }
        if topic_names[..index]
            .iter()
            .any(|previous| previous.topic_id == topic.topic_id)
        {
            return Err(GroupOffsetCommitPreparationErrorKind::DuplicateTopicId(
                topic.topic_id,
            ));
        }
        if topic_names[..index]
            .iter()
            .any(|previous| previous.name == topic.name)
        {
            return Err(GroupOffsetCommitPreparationErrorKind::DuplicateTopicName);
        }
    }
    Ok(())
}
