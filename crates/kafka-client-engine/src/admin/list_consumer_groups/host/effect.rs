//! Exact installation of deterministic submission and terminal effects.

use kafka_client_core::AdminListConsumerGroupsEffect;

use super::{
    ListConsumerGroupsHandoff, ListConsumerGroupsHost, ListConsumerGroupsHostError,
    ListConsumerGroupsSubmission, ListConsumerGroupsSubmissionKind,
};

impl ListConsumerGroupsHost {
    pub(super) fn install_effect(
        &mut self,
        index: usize,
        effect: AdminListConsumerGroupsEffect,
    ) -> Result<(), ListConsumerGroupsHostError> {
        let operation_id = self.operations[index].operation_id;
        let (effect_id, kind) = match effect {
            AdminListConsumerGroupsEffect::SubmitDiscovery {
                operation_id,
                deadline,
            } => {
                if deadline != self.operations[index].deadline.core() {
                    return Err(ListConsumerGroupsHostError::SubmissionMismatch);
                }
                (operation_id, ListConsumerGroupsSubmissionKind::Discovery)
            }
            AdminListConsumerGroupsEffect::SubmitBroker {
                operation_id,
                deadline,
                broker_id,
                filters,
            } => {
                if deadline != self.operations[index].deadline.core() {
                    return Err(ListConsumerGroupsHostError::SubmissionMismatch);
                }
                (
                    operation_id,
                    ListConsumerGroupsSubmissionKind::Broker {
                        broker_id,
                        filters,
                        retained_limit: self.operations[index].remaining_result_bytes,
                    },
                )
            }
            AdminListConsumerGroupsEffect::Complete {
                operation_id: effect_id,
                terminal,
            } => {
                if effect_id != operation_id {
                    return Err(ListConsumerGroupsHostError::SubmissionMismatch);
                }
                self.operations[index].terminal = Some(terminal);
                return self.publish_terminal(index);
            }
        };
        if effect_id != operation_id {
            return Err(ListConsumerGroupsHostError::SubmissionMismatch);
        }
        self.operations[index].submission = Some(ListConsumerGroupsSubmission {
            operation_id,
            deadline: self.operations[index].deadline,
            kind,
        });
        self.operations[index].handoff = ListConsumerGroupsHandoff::Untouched;
        Ok(())
    }
}
