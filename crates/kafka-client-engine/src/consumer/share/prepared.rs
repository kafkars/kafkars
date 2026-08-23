//! Exact engine deadline and core fences for one prepared share heartbeat.

use kafka_client_core::{
    AssignmentGeneration, ShareGroupHeartbeatAttempt, ShareGroupHeartbeatRequestKind,
    ShareGroupMemberEpoch,
};

use crate::clock::OperationDeadline;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PreparedShareGroupHeartbeat {
    pub(super) attempt: ShareGroupHeartbeatAttempt,
    pub(super) kind: ShareGroupHeartbeatRequestKind,
    pub(super) member_epoch: Option<ShareGroupMemberEpoch>,
    pub(super) assignment_generation: Option<AssignmentGeneration>,
    pub(super) deadline: OperationDeadline,
}
