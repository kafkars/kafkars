//! Linear operation, handoff, and turn ownership for Admin `ListOffsets`.

use kafka_client_core::{AdminListOffsetsMachine, AdminListOffsetsTerminal, OperationId};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{
        AdminListOffsetsCall, AdminListOffsetsTerminal as RawAdminListOffsetsTerminal,
        RecoveredAdminListOffsetsCall,
    },
};

use super::submission::AdminListOffsetsSubmission;

pub(crate) enum AdminListOffsetsTurn {
    Idle,
    Progress,
    Submit(AdminListOffsetsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AdminListOffsetsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct AdminListOffsetsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: AdminListOffsetsMachine,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) submission: Option<AdminListOffsetsSubmission>,
    pub(super) handoff: AdminListOffsetsHandoff,
    pub(super) call: Option<AdminListOffsetsCall>,
    pub(super) recovered_call: Option<RecoveredAdminListOffsetsCall>,
    pub(super) raw_terminal: Option<RawAdminListOffsetsTerminal>,
    pub(super) terminal: Option<AdminListOffsetsTerminal>,
}
