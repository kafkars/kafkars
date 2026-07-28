//! Atomic reservation and deterministic admission into one fixed slot.

use std::sync::Arc;

use kafka_client_core::{
    ProducerRetryPolicy, TransactionOffsetCommitEffect, TransactionOffsetCommitEndBarrier,
    TransactionOffsetCommitMachine, TransactionOffsetCommitMachineError,
    TransactionOffsetCommitStage, TransactionalProducerIdentity,
};

use crate::{
    completion::{CompletionId, CompletionRegistry, CompletionRegistryError},
    transaction::offset_commit::completion::TransactionOffsetCommitPublisher,
};

use super::{
    input::TransactionOffsetCommitRequest,
    model::{
        TransactionOffsetCommitAccepted, TransactionOffsetCommitAdmissionError,
        TransactionOffsetCommitAdmissionErrorKind, TransactionOffsetCommitHostError,
        TransactionOffsetCommitResult,
    },
    turn::{PendingTransactionOffsetCommit, TransactionOffsetCommitSlot},
};

pub(crate) struct TransactionOffsetCommitOwner {
    pub(super) machine: TransactionOffsetCommitMachine,
    pub(super) slot: TransactionOffsetCommitSlot,
    pub(super) completions:
        CompletionRegistry<TransactionOffsetCommitResult, TransactionOffsetCommitPublisher>,
    pub(super) reclaim_pending: Option<CompletionId>,
    pub(super) retry_policy: ProducerRetryPolicy,
    offset_limit: usize,
    retained_byte_limit: usize,
}

impl TransactionOffsetCommitOwner {
    pub(in crate::transaction) fn new(
        offset_limit: usize,
        retained_byte_limit: usize,
        retry_policy: ProducerRetryPolicy,
        publisher: TransactionOffsetCommitPublisher,
    ) -> Self {
        Self {
            machine: TransactionOffsetCommitMachine::new(),
            slot: TransactionOffsetCommitSlot::Vacant,
            completions: CompletionRegistry::with_publisher(1, publisher),
            reclaim_pending: None,
            retry_policy,
            offset_limit,
            retained_byte_limit,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "admission rejection returns the exact assignment-fenced offset request"
    )]
    pub(in crate::transaction) fn try_admit(
        &mut self,
        request: TransactionOffsetCommitRequest,
        transactional_id: Arc<str>,
        producer: TransactionalProducerIdentity,
    ) -> Result<TransactionOffsetCommitAccepted, TransactionOffsetCommitAdmissionError> {
        if !matches!(self.slot, TransactionOffsetCommitSlot::Vacant) {
            return Err(rejected(
                TransactionOffsetCommitAdmissionErrorKind::Busy,
                request,
            ));
        }
        if let Some(kind) = super::validation::validate_request(
            &request,
            self.offset_limit,
            self.retained_byte_limit,
        ) {
            return Err(rejected(kind, request));
        }
        let (completion_id, observer) = match self.completions.reserve() {
            Ok(reservation) => reservation,
            Err(error) => return Err(rejected(reservation_failure(error), request)),
        };
        let transition = match self.machine.admit(
            request.epoch(),
            request.deadline().core(),
            request.group().fence(),
        ) {
            Ok(transition) => transition,
            Err(error) => {
                let kind = core_admission_failure(error);
                if self
                    .completions
                    .rollback_reservation(completion_id)
                    .is_err()
                {
                    return Err(rejected(
                        TransactionOffsetCommitAdmissionErrorKind::CompletionCapacity,
                        request,
                    ));
                }
                return Err(rejected(kind, request));
            }
        };
        let operation_id = self
            .machine
            .operation_id()
            .unwrap_or_else(|| unreachable!("admission retains one operation identity"));
        match transition.into_effect() {
            Some(TransactionOffsetCommitEffect::SubmitAddOffsets {
                epoch,
                operation_id: effect_id,
                deadline,
                group_fence,
            }) if epoch == request.epoch()
                && effect_id == operation_id
                && deadline == request.deadline().core()
                && group_fence == request.group().fence() => {}
            _ => unreachable!("offset admission emits its exact AddOffsets effect"),
        }
        self.slot = TransactionOffsetCommitSlot::Ready(
            PendingTransactionOffsetCommit {
                completion_id,
                operation_id,
                transactional_id,
                producer,
                request,
                retry_not_before: None,
                retries_started: 0,
            },
            TransactionOffsetCommitStage::AddOffsets,
        );
        Ok(TransactionOffsetCommitAccepted::new(operation_id, observer))
    }

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        match &self.slot {
            TransactionOffsetCommitSlot::Ready(pending, _) => {
                Some(pending.retry_not_before.map_or_else(
                    || pending.request.deadline().core(),
                    |not_before| not_before.min(pending.request.deadline().core()),
                ))
            }
            TransactionOffsetCommitSlot::Calling(pending, _, _) => {
                Some(pending.request.deadline().core())
            }
            _ => None,
        }
    }

    pub(in crate::transaction) fn preflight_end(
        &self,
        epoch: kafka_client_core::TransactionEpoch,
    ) -> Result<TransactionOffsetCommitEndBarrier, TransactionOffsetCommitMachineError> {
        self.machine.preflight_end(epoch)
    }

    pub(in crate::transaction) fn has_unsettled_barrier(&self) -> bool {
        self.machine.operation_id().is_some()
    }

    pub(crate) fn unsettled(&self) -> usize {
        usize::from(!matches!(
            self.slot,
            TransactionOffsetCommitSlot::Vacant | TransactionOffsetCommitSlot::Published
        ))
    }

    pub(crate) fn is_releasable_after_owner_close(&self) -> bool {
        matches!(
            self.slot,
            TransactionOffsetCommitSlot::Vacant | TransactionOffsetCommitSlot::Published
        )
    }
}

const fn reservation_failure(
    _error: CompletionRegistryError,
) -> TransactionOffsetCommitAdmissionErrorKind {
    TransactionOffsetCommitAdmissionErrorKind::CompletionCapacity
}

const fn core_admission_failure(
    error: TransactionOffsetCommitMachineError,
) -> TransactionOffsetCommitAdmissionErrorKind {
    match error {
        TransactionOffsetCommitMachineError::IdentityExhausted => {
            TransactionOffsetCommitAdmissionErrorKind::IdentityExhausted
        }
        _ => TransactionOffsetCommitAdmissionErrorKind::InvalidLifecycle,
    }
}

const fn rejected(
    kind: TransactionOffsetCommitAdmissionErrorKind,
    request: TransactionOffsetCommitRequest,
) -> TransactionOffsetCommitAdmissionError {
    TransactionOffsetCommitAdmissionError::new(kind, request)
}

impl From<CompletionRegistryError> for TransactionOffsetCommitHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl From<TransactionOffsetCommitMachineError> for TransactionOffsetCommitHostError {
    fn from(error: TransactionOffsetCommitMachineError) -> Self {
        Self::Core(error)
    }
}
