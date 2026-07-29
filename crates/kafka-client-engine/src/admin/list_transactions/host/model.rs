//! Pending and prepared discovery or exact-broker submission ownership.

use kafka_client_core::{AdminListTransactionsPlan, OperationId};

use crate::clock::OperationDeadline;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AdminListTransactionsSubmissionKind {
    Discovery {
        retained_limit: usize,
    },
    Broker {
        broker_id: i32,
        plan: AdminListTransactionsPlan,
        retained_limit: usize,
    },
}

pub(crate) struct AdminListTransactionsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) kind: AdminListTransactionsSubmissionKind,
}

impl AdminListTransactionsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        AdminListTransactionsSubmissionKind,
    ) {
        (self.operation_id, self.deadline, self.kind)
    }
}

pub(crate) enum AdminListTransactionsTurn {
    Idle,
    Progress,
    Submit(AdminListTransactionsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AdminListTransactionsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
