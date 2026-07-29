//! Exact installation of deterministic discovery, broker, and terminal effects.

use kafka_client_core::AdminListTransactionsEffect;

use super::{
    AdminListTransactionsHandoff, AdminListTransactionsHost, AdminListTransactionsHostError,
    AdminListTransactionsSubmission, AdminListTransactionsSubmissionKind,
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
                (operation_id, AdminListTransactionsSubmissionKind::Discovery)
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
        self.operations[index].submission = Some(AdminListTransactionsSubmission {
            operation_id,
            deadline: self.operations[index].deadline,
            kind,
        });
        self.operations[index].handoff = AdminListTransactionsHandoff::Untouched;
        Ok(())
    }
}
