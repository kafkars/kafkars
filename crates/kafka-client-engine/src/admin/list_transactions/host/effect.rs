//! Exact installation of deterministic discovery, broker, and terminal effects.

use kafka_client_core::AdminListTransactionsEffect;

use super::{
    AdminListTransactionsHandoff, AdminListTransactionsHost, AdminListTransactionsHostError,
    AdminListTransactionsOperation, AdminListTransactionsSubmission,
    AdminListTransactionsSubmissionKind,
};

impl AdminListTransactionsHost {
    pub(super) fn install_effect(
        &mut self,
        index: usize,
        effect: AdminListTransactionsEffect,
    ) -> Result<(), AdminListTransactionsHostError> {
        let operation_id = self.operations[index].operation_id;
        let (effect_id, kind) = match effect {
            AdminListTransactionsEffect::SubmitDiscovery {
                operation_id,
                deadline,
            } => {
                if deadline != self.operations[index].deadline.core() {
                    return Err(AdminListTransactionsHostError::SubmissionMismatch);
                }
                (
                    operation_id,
                    AdminListTransactionsSubmissionKind::Discovery {
                        retained_limit: self.operations[index].remaining_result_bytes,
                    },
                )
            }
            AdminListTransactionsEffect::SubmitBroker {
                operation_id,
                deadline,
                broker_id,
                plan,
            } => {
                if deadline != self.operations[index].deadline.core() {
                    return Err(AdminListTransactionsHostError::SubmissionMismatch);
                }
                (
                    operation_id,
                    AdminListTransactionsSubmissionKind::Broker {
                        broker_id,
                        plan,
                        retained_limit: self.operations[index].remaining_result_bytes,
                    },
                )
            }
            AdminListTransactionsEffect::Complete {
                operation_id: effect_id,
                terminal,
            } => {
                if effect_id != operation_id {
                    return Err(AdminListTransactionsHostError::SubmissionMismatch);
                }
                self.operations[index].terminal = Some(terminal);
                return self.publish_terminal(index);
            }
        };
        if effect_id != operation_id {
            return Err(AdminListTransactionsHostError::SubmissionMismatch);
        }
        self.operations[index].prepare_submission(kind);
        Ok(())
    }
}

impl AdminListTransactionsOperation {
    pub(super) fn prepare_submission(&mut self, kind: AdminListTransactionsSubmissionKind) {
        self.active_submission = Some(kind.clone());
        self.submission = Some(AdminListTransactionsSubmission {
            operation_id: self.operation_id,
            deadline: self.deadline,
            kind,
        });
        self.handoff = AdminListTransactionsHandoff::Untouched;
    }
}
