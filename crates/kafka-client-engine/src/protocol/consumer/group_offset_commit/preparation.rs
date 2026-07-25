//! Recoverable ownership and scalar reasons for failed commit preparation.

use kafka_client_core::{Deadline, GroupOffsetCommitEffect, PartitionIndex, TopicId};

use crate::clock::OperationDeadline;

use super::{
    result_reservation::GroupOffsetCommitResultReservation,
    session::{ClassicGroupCommitSession, GroupOffsetCommitTopicName},
};

/// Scalar reason one preparation attempt failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitPreparationErrorKind {
    UnexpectedEffect,
    DeadlineMismatch {
        effect: Deadline,
        operation: Deadline,
    },
    GroupMismatch,
    MemberMismatch,
    GenerationMismatch,
    ClassicGenerationOutOfRange,
    EmptyGroup,
    GroupTooLong {
        actual: usize,
        limit: usize,
    },
    EmptyMember,
    MemberTooLong {
        actual: usize,
        limit: usize,
    },
    EntryCapacity {
        actual: usize,
        limit: usize,
    },
    TopicCapacity {
        actual: usize,
        limit: usize,
    },
    ResultReservationMismatch {
        entries: usize,
        reserved: usize,
    },
    AllocationFailed,
    EmptyTopicName,
    TopicNameTooLong {
        actual: usize,
        limit: usize,
    },
    DuplicateTopicId(TopicId),
    DuplicateTopicName,
    UnknownTopic(TopicId),
    UnusedTopic,
    PartitionOutOfRange {
        topic_id: TopicId,
        partition: PartitionIndex,
    },
}

/// Recoverable preparation failure retaining every consumed linear owner.
#[must_use = "preparation failure retains the exact group commit owners"]
#[derive(Debug)]
pub(crate) struct GroupOffsetCommitPreparationError {
    kind: GroupOffsetCommitPreparationErrorKind,
    effect: GroupOffsetCommitEffect,
    operation_deadline: OperationDeadline,
    session: ClassicGroupCommitSession,
    topic_names: Vec<GroupOffsetCommitTopicName>,
    result_reservation: GroupOffsetCommitResultReservation,
}

impl GroupOffsetCommitPreparationError {
    pub(super) const fn new(
        kind: GroupOffsetCommitPreparationErrorKind,
        effect: GroupOffsetCommitEffect,
        operation_deadline: OperationDeadline,
        session: ClassicGroupCommitSession,
        topic_names: Vec<GroupOffsetCommitTopicName>,
        result_reservation: GroupOffsetCommitResultReservation,
    ) -> Self {
        Self {
            kind,
            effect,
            operation_deadline,
            session,
            topic_names,
            result_reservation,
        }
    }

    pub(crate) const fn kind(&self) -> GroupOffsetCommitPreparationErrorKind {
        self.kind
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        GroupOffsetCommitEffect,
        OperationDeadline,
        ClassicGroupCommitSession,
        Vec<GroupOffsetCommitTopicName>,
        GroupOffsetCommitResultReservation,
    ) {
        (
            self.effect,
            self.operation_deadline,
            self.session,
            self.topic_names,
            self.result_reservation,
        )
    }
}
