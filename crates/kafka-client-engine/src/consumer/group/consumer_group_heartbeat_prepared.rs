//! Exact core-authorized KIP-848 submission facts retained for mechanism ownership.

use kafka_client_core::{
    AssignmentGeneration, ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatRequestKind,
    ConsumerGroupMemberEpoch, MemberId,
};

use crate::clock::OperationDeadline;

/// One exact core-authorized API 68 submission awaiting mechanism ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PreparedConsumerGroupHeartbeat {
    pub(super) attempt: ConsumerGroupHeartbeatAttempt,
    pub(super) kind: ConsumerGroupHeartbeatRequestKind,
    pub(super) member_id: Option<MemberId>,
    pub(super) member_epoch: Option<ConsumerGroupMemberEpoch>,
    pub(super) assignment_generation: Option<AssignmentGeneration>,
    pub(super) deadline: OperationDeadline,
}

impl PreparedConsumerGroupHeartbeat {
    pub(super) const fn attempt(self) -> ConsumerGroupHeartbeatAttempt {
        self.attempt
    }

    pub(super) const fn kind(self) -> ConsumerGroupHeartbeatRequestKind {
        self.kind
    }

    pub(super) const fn member_id(self) -> Option<MemberId> {
        self.member_id
    }

    pub(super) const fn member_epoch(self) -> Option<ConsumerGroupMemberEpoch> {
        self.member_epoch
    }

    pub(super) const fn assignment_generation(self) -> Option<AssignmentGeneration> {
        self.assignment_generation
    }

    pub(super) const fn deadline(self) -> OperationDeadline {
        self.deadline
    }
}
