//! Atomic execution-host checks before one fixed transactional send handoff.

use kafka_client_core::TransactionalOwnerId;

use crate::transaction::send::{
    TransactionSendAccepted, TransactionSendInput, TransactionSendRequest,
};

use super::{
    host::TransactionExecutionHost,
    model::{TransactionExecutionSendAdmissionError, TransactionExecutionSendAdmissionErrorKind},
};

impl TransactionExecutionHost {
    #[expect(
        clippy::result_large_err,
        reason = "send rejection returns the exact caller-owned transactional input"
    )]
    pub(crate) fn try_send(
        &mut self,
        owner_id: TransactionalOwnerId,
        input: TransactionSendInput,
    ) -> Result<TransactionSendAccepted, TransactionExecutionSendAdmissionError> {
        if !self.owns(owner_id) {
            return Err(TransactionExecutionSendAdmissionError::new(
                TransactionExecutionSendAdmissionErrorKind::StaleOwner,
                input,
            ));
        }
        let record_count = input.record_count();
        if record_count > self.batch_record_capacity {
            return Err(TransactionExecutionSendAdmissionError::new(
                TransactionExecutionSendAdmissionErrorKind::BatchRecordCapacity {
                    actual: record_count,
                    limit: self.batch_record_capacity,
                },
                input,
            ));
        }
        let retained_source_bytes = input.retained_source_bytes();
        if retained_source_bytes > self.retained_record_byte_limit {
            return Err(TransactionExecutionSendAdmissionError::new(
                TransactionExecutionSendAdmissionErrorKind::RetainedRecordBytes {
                    actual: retained_source_bytes,
                    limit: self.retained_record_byte_limit,
                },
                input,
            ));
        }
        let prepared_topic = match self.topics.prepare(input.canonical_topic()) {
            Ok(prepared) => prepared,
            Err(error) => {
                return Err(TransactionExecutionSendAdmissionError::new(
                    error.into(),
                    input,
                ));
            }
        };
        let request = match TransactionSendRequest::try_prepare(
            input,
            prepared_topic.topic_id(),
            self.max_wire_batch_bytes,
        ) {
            Ok(request) => request,
            Err(input) => {
                return Err(TransactionExecutionSendAdmissionError::new(
                    TransactionExecutionSendAdmissionErrorKind::Allocation,
                    input,
                ));
            }
        };
        match self.send.try_send(&mut self.lifecycle, request) {
            Ok(accepted) => {
                self.topics.commit(prepared_topic);
                Ok(accepted)
            }
            Err(failure) => {
                let kind = TransactionExecutionSendAdmissionErrorKind::Send(failure.kind());
                Err(TransactionExecutionSendAdmissionError::new(
                    kind,
                    failure.into_input(),
                ))
            }
        }
    }
}
