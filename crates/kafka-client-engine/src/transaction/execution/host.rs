//! One installed transaction lifecycle composed with one fixed send slot.

use kafka_client_core::{
    TransactionEndMode, TransactionEpoch, TransactionLifecycleTerminal,
    TransactionOffsetCommitEndBarrier, TransactionalOwnerId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionObserver,
    transaction::{
        TransactionExecutionLimits, TransactionLifecycleHost, TransactionLifecycleHostError,
        initialization::TransactionalOwnerParts,
        offset_commit::{
            TransactionOffsetCommitAccepted, TransactionOffsetCommitAdmissionError,
            TransactionOffsetCommitAdmissionErrorKind, TransactionOffsetCommitOwner,
            TransactionOffsetCommitRequest,
        },
        send::{
            TransactionSendAccepted, TransactionSendInput, TransactionSendOwner,
            TransactionSendRequest,
        },
    },
};

use super::{
    model::{TransactionExecutionSendAdmissionError, TransactionExecutionSendAdmissionErrorKind},
    topic_catalog::TransactionTopicCatalog,
};

/// Unique execution owner installed after producer identity initialization.
pub(crate) struct TransactionExecutionHost {
    pub(super) lifecycle: TransactionLifecycleHost,
    pub(super) send: TransactionSendOwner,
    pub(super) offset_commit: TransactionOffsetCommitOwner,
    pub(super) topics: TransactionTopicCatalog,
    retained_record_byte_limit: usize,
    max_wire_batch_bytes: usize,
    pub(super) owner_loss_pending: Option<OperationDeadline>,
}

impl TransactionExecutionHost {
    #[allow(
        clippy::result_large_err,
        reason = "failed installation returns the exact initialized transactional owner for recovery"
    )]
    pub(in crate::transaction) fn try_new(
        parts: TransactionalOwnerParts,
        limits: TransactionExecutionLimits,
    ) -> Result<Self, (TransactionLifecycleHostError, TransactionalOwnerParts)> {
        let topics = TransactionTopicCatalog::new(
            limits.partition_capacity(),
            limits.retained_topic_bytes(),
        );
        TransactionLifecycleHost::try_new(parts, limits).map(|mut lifecycle| {
            let send_publisher = lifecycle.take_send_publisher();
            let offset_commit_publisher = lifecycle.take_offset_commit_publisher();
            Self {
                lifecycle,
                send: TransactionSendOwner::new(
                    limits.compression(),
                    limits.partition_capacity(),
                    send_publisher,
                ),
                offset_commit: TransactionOffsetCommitOwner::new(
                    limits.transaction_offset_count(),
                    limits.transaction_offset_bytes(),
                    limits.send_retry_policy(),
                    offset_commit_publisher,
                ),
                topics,
                retained_record_byte_limit: limits.retained_record_bytes(),
                max_wire_batch_bytes: limits.max_wire_batch_bytes(),
                owner_loss_pending: None,
            }
        })
    }

    pub(crate) fn owns(&self, owner_id: TransactionalOwnerId) -> bool {
        self.lifecycle.owns(owner_id)
    }

    pub(crate) fn begin(&mut self) -> Result<TransactionEpoch, TransactionLifecycleHostError> {
        self.lifecycle.begin()
    }

    pub(crate) fn end(
        &mut self,
        epoch: TransactionEpoch,
        mode: TransactionEndMode,
        deadline: OperationDeadline,
    ) -> Result<CompletionObserver<TransactionLifecycleTerminal>, TransactionLifecycleHostError>
    {
        if !matches!(
            self.offset_commit.preflight_end(epoch)?,
            TransactionOffsetCommitEndBarrier::Ready
        ) {
            return Err(TransactionLifecycleHostError::OffsetCommitUnsettled);
        }
        match mode {
            TransactionEndMode::Commit => self.lifecycle.commit(epoch, deadline),
            TransactionEndMode::Abort => self.lifecycle.abort(epoch, deadline),
        }
    }

    pub(crate) fn owner_lost(
        &mut self,
        deadline: OperationDeadline,
    ) -> Result<(), TransactionLifecycleHostError> {
        if self.offset_commit.has_unsettled_barrier() {
            self.owner_loss_pending.get_or_insert(deadline);
            return Ok(());
        }
        self.lifecycle.owner_lost(deadline)
    }

    pub(crate) fn idle_owner_lost(&mut self) -> Result<(), TransactionLifecycleHostError> {
        self.lifecycle.idle_owner_lost()
    }

    #[expect(
        clippy::result_large_err,
        reason = "send rejection returns the exact caller-owned transactional record"
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

    #[expect(
        clippy::result_large_err,
        reason = "offset rejection returns the exact assignment-fenced request"
    )]
    pub(crate) fn try_offset_commit(
        &mut self,
        request: TransactionOffsetCommitRequest,
    ) -> Result<TransactionOffsetCommitAccepted, TransactionOffsetCommitAdmissionError> {
        if !self.owns(request.owner_id()) {
            return Err(TransactionOffsetCommitAdmissionError::new(
                TransactionOffsetCommitAdmissionErrorKind::StaleOwner,
                request,
            ));
        }
        let identity = self
            .lifecycle
            .offset_commit_identity(request.owner_id(), request.epoch());
        let Ok((transactional_id, producer)) = identity else {
            return Err(TransactionOffsetCommitAdmissionError::new(
                TransactionOffsetCommitAdmissionErrorKind::InvalidLifecycle,
                request,
            ));
        };
        self.offset_commit
            .try_admit(request, transactional_id, producer)
    }

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        [
            self.send.next_deadline(),
            self.offset_commit.next_deadline(),
            self.lifecycle.next_deadline(),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    pub(crate) fn unsettled(&self) -> usize {
        self.send.unsettled() + self.offset_commit.unsettled() + self.lifecycle.unsettled()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.lifecycle.is_closed()
            && self.send.is_releasable_after_owner_close()
            && self.offset_commit.is_releasable_after_owner_close()
    }

    #[cfg(test)]
    pub(super) fn settle_pending_enrolled_for_test(&mut self) {
        self.lifecycle.settle_pending_enrolled_for_test();
    }

    #[cfg(test)]
    pub(super) fn topic_id_for_test(&self, topic: &str) -> Option<kafka_client_core::TopicId> {
        self.topics.topic_id(topic)
    }
}
