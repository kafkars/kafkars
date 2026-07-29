//! Exact installation of the sole deterministic submission or terminal effect.

use kafka_client_core::DescribeTopicPartitionsEffect;

use super::{
    AdminDescribeTopicPartitionsHandoff, AdminDescribeTopicPartitionsHost,
    AdminDescribeTopicPartitionsHostError, AdminDescribeTopicPartitionsSubmission,
};

impl AdminDescribeTopicPartitionsHost {
    pub(super) fn install_effect(
        &mut self,
        index: usize,
        effect: DescribeTopicPartitionsEffect,
    ) -> Result<(), AdminDescribeTopicPartitionsHostError> {
        let operation_id = self.operations[index].operation_id;
        match effect {
            DescribeTopicPartitionsEffect::Submit {
                operation_id: effect_id,
                deadline,
                plan,
            } => {
                if effect_id != operation_id || deadline != self.operations[index].deadline.core() {
                    return Err(AdminDescribeTopicPartitionsHostError::SubmissionMismatch);
                }
                self.operations[index].submission = Some(AdminDescribeTopicPartitionsSubmission {
                    operation_id,
                    deadline: self.operations[index].deadline,
                    plan,
                    retained_limit: self.operations[index].remaining_result_bytes,
                });
                self.operations[index].handoff = AdminDescribeTopicPartitionsHandoff::Untouched;
                Ok(())
            }
            DescribeTopicPartitionsEffect::Complete {
                operation_id: effect_id,
                terminal,
            } => {
                if effect_id != operation_id {
                    return Err(AdminDescribeTopicPartitionsHostError::SubmissionMismatch);
                }
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
        }
    }
}
