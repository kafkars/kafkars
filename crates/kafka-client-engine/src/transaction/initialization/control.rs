//! Bounded shard control for one initialized transactional lifecycle.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    TransactionEndMode, TransactionEpoch, TransactionLifecycleTerminal, TransactionalOwnerId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionObserver,
    producer::{ProducerSendCapture, ProducerSendCaptureError},
    transaction::{
        TransactionExecutionSendAdmissionError, TransactionExecutionSendAdmissionErrorKind,
        TransactionLifecycleHostError,
        offset_commit::{
            TransactionOffsetCommitAccepted, TransactionOffsetCommitAdmissionError,
            TransactionOffsetCommitAdmissionErrorKind, TransactionOffsetCommitRequest,
        },
        send::{TransactionSendAccepted as InternalTransactionSendAccepted, TransactionSendInput},
    },
};

use super::shard::TransactionInitializationShardState;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionLifecycleControlError {
    InvalidDeadline,
    Contended,
    Closed,
    StaleOwner,
    Host(TransactionLifecycleHostError),
}

pub(crate) struct TransactionLifecycleControlAccepted<T> {
    pub(crate) value: T,
    pub(crate) wake_failed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionSendControlErrorKind {
    Contended,
    Closed,
    Admission(TransactionExecutionSendAdmissionErrorKind),
}

pub(crate) struct TransactionSendControlError {
    kind: TransactionSendControlErrorKind,
    input: TransactionSendInput,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionOffsetCommitControlErrorKind {
    Contended,
    Closed,
    Admission(TransactionOffsetCommitAdmissionErrorKind),
}

pub(crate) struct TransactionOffsetCommitControlError {
    kind: TransactionOffsetCommitControlErrorKind,
    input: TransactionOffsetCommitRequest,
}

impl TransactionOffsetCommitControlError {
    pub(super) const fn local(
        kind: TransactionOffsetCommitControlErrorKind,
        input: TransactionOffsetCommitRequest,
    ) -> Self {
        Self { kind, input }
    }

    pub(super) fn admission(error: TransactionOffsetCommitAdmissionError) -> Self {
        Self {
            kind: TransactionOffsetCommitControlErrorKind::Admission(error.kind()),
            input: error.into_input(),
        }
    }

    pub(crate) const fn kind(&self) -> TransactionOffsetCommitControlErrorKind {
        self.kind
    }

    pub(crate) fn into_input(self) -> TransactionOffsetCommitRequest {
        self.input
    }
}

impl TransactionSendControlError {
    pub(super) const fn local(
        kind: TransactionSendControlErrorKind,
        input: TransactionSendInput,
    ) -> Self {
        Self { kind, input }
    }

    pub(super) fn admission(error: TransactionExecutionSendAdmissionError) -> Self {
        Self {
            kind: TransactionSendControlErrorKind::Admission(error.kind()),
            input: error.into_input(),
        }
    }

    pub(crate) const fn kind(&self) -> TransactionSendControlErrorKind {
        self.kind
    }

    pub(crate) fn into_input(self) -> TransactionSendInput {
        self.input
    }
}

pub(crate) struct TransactionOwnerLossSignal {
    pub(super) owner_id: TransactionalOwnerId,
    pub(super) deadline: Option<OperationDeadline>,
}

#[derive(Clone)]
pub(crate) struct TransactionLifecycleControlPort {
    shared: Arc<TransactionInitializationShardState>,
}

impl TransactionLifecycleControlPort {
    pub(super) const fn new(shared: Arc<TransactionInitializationShardState>) -> Self {
        Self { shared }
    }

    pub(crate) fn begin(
        &self,
        owner_id: TransactionalOwnerId,
    ) -> Result<
        TransactionLifecycleControlAccepted<TransactionEpoch>,
        TransactionLifecycleControlError,
    > {
        let epoch = self.shared.try_begin(owner_id)?;
        Ok(TransactionLifecycleControlAccepted {
            value: epoch,
            wake_failed: self.shared.wake().request().is_err(),
        })
    }

    pub(crate) fn capture_send(
        &self,
        timeout: Duration,
    ) -> Result<ProducerSendCapture, ProducerSendCaptureError> {
        ProducerSendCapture::capture_transaction(self.shared.clock(), timeout)
    }

    pub(crate) fn capture_offset_commit(&self, timeout: Duration) -> Option<OperationDeadline> {
        self.shared
            .clock()
            .capture_deadline_after(timeout)
            .ok()
            .filter(|_| !timeout.is_zero())
            .map(crate::clock::DeadlineCapture::operation_deadline)
    }

    #[expect(
        clippy::result_large_err,
        reason = "control rejection returns the exact caller-owned transactional record"
    )]
    pub(crate) fn send(
        &self,
        owner_id: TransactionalOwnerId,
        input: TransactionSendInput,
    ) -> Result<
        TransactionLifecycleControlAccepted<InternalTransactionSendAccepted>,
        TransactionSendControlError,
    > {
        let accepted = self.shared.try_send(owner_id, input)?;
        Ok(TransactionLifecycleControlAccepted {
            value: accepted,
            wake_failed: self.shared.wake().request().is_err(),
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "control rejection returns the exact assignment-fenced offset request"
    )]
    pub(crate) fn send_offsets(
        &self,
        owner_id: TransactionalOwnerId,
        input: TransactionOffsetCommitRequest,
    ) -> Result<
        TransactionLifecycleControlAccepted<TransactionOffsetCommitAccepted>,
        TransactionOffsetCommitControlError,
    > {
        let accepted = self.shared.try_offset_commit(owner_id, input)?;
        Ok(TransactionLifecycleControlAccepted {
            value: accepted,
            wake_failed: self.shared.wake().request().is_err(),
        })
    }

    pub(crate) fn end(
        &self,
        owner_id: TransactionalOwnerId,
        epoch: TransactionEpoch,
        mode: TransactionEndMode,
        timeout: Duration,
    ) -> Result<
        TransactionLifecycleControlAccepted<CompletionObserver<TransactionLifecycleTerminal>>,
        TransactionLifecycleControlError,
    > {
        let deadline = self
            .shared
            .clock()
            .capture_deadline_after(timeout)
            .ok()
            .filter(|_| !timeout.is_zero())
            .map(crate::clock::DeadlineCapture::operation_deadline)
            .ok_or(TransactionLifecycleControlError::InvalidDeadline)?;
        let observer = self.shared.try_end(owner_id, epoch, mode, deadline)?;
        Ok(TransactionLifecycleControlAccepted {
            value: observer,
            wake_failed: self.shared.wake().request().is_err(),
        })
    }

    pub(super) fn owner_lost(&self, owner_id: TransactionalOwnerId, timeout: Duration) {
        let deadline = self
            .shared
            .clock()
            .capture_deadline_after(timeout)
            .ok()
            .map(crate::clock::DeadlineCapture::operation_deadline);
        self.shared
            .enqueue_owner_loss(TransactionOwnerLossSignal { owner_id, deadline });
    }
}
