//! Exact submission, call-evidence, and operation ownership for one ACL query.

use kafka_client_core::{DescribeAclsMachine, DescribeAclsPlan, DescribeAclsTerminal, OperationId};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{DescribeAclsCall, DescribeAclsRawTerminal, RecoveredDescribeAclsCall},
};

pub(crate) struct DescribeAclsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: DescribeAclsPlan,
    pub(super) result_limit: usize,
}

impl DescribeAclsSubmission {
    pub(crate) fn into_parts(self) -> (OperationId, OperationDeadline, DescribeAclsPlan, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.result_limit,
        )
    }
}

pub(crate) enum DescribeAclsTurn {
    Idle,
    Progress,
    Submit(DescribeAclsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DescribeAclsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct DescribeAclsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: DescribeAclsMachine,
    pub(super) plan: DescribeAclsPlan,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) result_limit: usize,
    pub(super) remaining_result_bytes: usize,
    pub(super) submission: Option<DescribeAclsSubmission>,
    pub(super) handoff: DescribeAclsHandoff,
    pub(super) call: Option<DescribeAclsCall>,
    pub(super) recovered_call: Option<RecoveredDescribeAclsCall>,
    pub(super) raw_terminal: Option<DescribeAclsRawTerminal>,
    pub(super) terminal: Option<DescribeAclsTerminal>,
}

impl DescribeAclsOperation {
    pub(super) fn matches_submission(&self, plan: &DescribeAclsPlan, result_limit: usize) -> bool {
        self.plan == *plan && self.result_limit == result_limit
    }

    pub(super) fn matches_call(&self, call: &DescribeAclsCall) -> bool {
        call.matches(&self.plan, self.result_limit)
    }

    pub(super) fn matches_raw(&self, raw: &DescribeAclsRawTerminal) -> bool {
        raw.matches(&self.plan, self.result_limit)
    }

    pub(super) fn matches_recovered(&self, recovered: &RecoveredDescribeAclsCall) -> bool {
        recovered.matches(&self.plan, self.result_limit)
    }
}
