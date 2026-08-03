//! Linear public transactional offset-transfer admission.

use std::{fmt, time::Duration};

use crate::{
    clock::OperationDeadline,
    consumer::{GroupConsumerCheckpoint, GroupConsumerMetadata},
    transaction::offset_commit::{
        TransactionOffsetCommitGroup, TransactionOffsetCommitOffset, TransactionOffsetCommitRequest,
    },
};

use super::{
    TransactionLifecycleControlAccepted, TransactionOffsetsAdmissionError,
    TransactionOffsetsAdmissionErrorKind, TransactionOffsetsObserver, TransactionToken,
    offset_admission::control_error_kind,
};

impl<'owner> TransactionToken<'owner> {
    /// Captures one transactional offset-transfer boundary before input translation.
    pub fn capture_offsets(&self, timeout: Duration) -> Option<TransactionOffsetsCapture> {
        self.owner
            .capture_offset_commit(timeout)
            .map(TransactionOffsetsCapture::new)
    }

    /// Admits one exact assignment-fenced checkpoint into this transaction.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns the exact linear metadata and checkpoint inputs"
    )]
    pub fn send_offsets<'send>(
        &'send mut self,
        metadata: GroupConsumerMetadata,
        checkpoint: GroupConsumerCheckpoint,
        timeout: Duration,
    ) -> Result<TransactionOffsetsAccepted<'send, 'owner>, TransactionOffsetsAdmissionError> {
        let Some(capture) = self.capture_offsets(timeout) else {
            return Err(TransactionOffsetsAdmissionError::new(
                TransactionOffsetsAdmissionErrorKind::InvalidDeadline,
                metadata,
                checkpoint,
            ));
        };
        self.send_offsets_captured(metadata, checkpoint, capture)
    }

    /// Admits exact offset inputs under an already captured public boundary.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns the exact linear metadata and checkpoint inputs"
    )]
    pub fn send_offsets_captured<'send>(
        &'send mut self,
        metadata: GroupConsumerMetadata,
        checkpoint: GroupConsumerCheckpoint,
        capture: TransactionOffsetsCapture,
    ) -> Result<TransactionOffsetsAccepted<'send, 'owner>, TransactionOffsetsAdmissionError> {
        let deadline = capture.into_deadline();
        if metadata.position_fence() != checkpoint.position_fence() {
            return Err(TransactionOffsetsAdmissionError::new(
                TransactionOffsetsAdmissionErrorKind::StaleCheckpoint,
                metadata,
                checkpoint,
            ));
        }
        let mut offsets = Vec::new();
        if offsets.try_reserve_exact(1).is_err() {
            return Err(TransactionOffsetsAdmissionError::new(
                TransactionOffsetsAdmissionErrorKind::Backpressure,
                metadata,
                checkpoint,
            ));
        }
        let (topic, partition, next_offset, leader_epoch) = checkpoint.transaction_offset();
        offsets.push(TransactionOffsetCommitOffset::new(
            Arc::clone(topic),
            partition,
            next_offset,
            leader_epoch,
            None,
        ));
        let request = TransactionOffsetCommitRequest::new(
            self.owner.owner_id(),
            self.epoch,
            TransactionOffsetCommitGroup::new(
                metadata.group_arc(),
                metadata.generation_id_or_member_epoch(),
                metadata.member_arc(),
                metadata.group_instance_id_arc(),
                metadata.position_fence(),
            ),
            offsets,
            deadline,
        );
        let TransactionLifecycleControlAccepted { value, wake_failed } =
            match self.owner.send_offsets(request) {
                Ok(accepted) => accepted,
                Err(error) => {
                    let kind = control_error_kind(&error);
                    drop(error.into_input());
                    return Err(TransactionOffsetsAdmissionError::new(
                        kind, metadata, checkpoint,
                    ));
                }
            };
        let operation_id = value.operation_id();
        Ok(TransactionOffsetsAccepted {
            observer: TransactionOffsetsObserver::new(value.into_observer(), self, operation_id),
            wake_failed,
        })
    }
}

use std::sync::Arc;

/// One non-cloneable transactional offset-transfer call boundary.
#[must_use = "consume the captured boundary with one transactional offset transfer"]
#[derive(Debug)]
pub struct TransactionOffsetsCapture {
    deadline: OperationDeadline,
}

impl TransactionOffsetsCapture {
    const fn new(deadline: OperationDeadline) -> Self {
        Self { deadline }
    }

    const fn into_deadline(self) -> OperationDeadline {
        self.deadline
    }
}

/// Accepted transfer ownership plus advisory post-admission wake status.
#[must_use = "accepted transactional offsets retain their sole terminal observer"]
pub struct TransactionOffsetsAccepted<'send, 'owner> {
    observer: TransactionOffsetsObserver<'send, 'owner>,
    wake_failed: bool,
}

impl<'send, 'owner> TransactionOffsetsAccepted<'send, 'owner> {
    /// Reports that the advisory reactor wake failed after acceptance.
    pub const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    /// Transfers the sole runtime-neutral terminal observer.
    pub fn into_observer(self) -> TransactionOffsetsObserver<'send, 'owner> {
        self.observer
    }
}

impl fmt::Debug for TransactionOffsetsAccepted<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionOffsetsAccepted")
            .field("observer", &self.observer)
            .field("wake_failed", &self.wake_failed)
            .finish()
    }
}
