//! Fixed terminal reservation and atomic deterministic send acceptance.

use kafka_client_core::{
    CompressionPolicy, DeliveryStatus, TransactionSendId, TransactionSendOutcome,
};

use crate::{
    completion::{CompletionId, CompletionRegistry},
    producer::materialization::TransactionalMaterializationBatch,
    transaction::{
        TransactionLifecycleHost, completion::TransactionSendPublisher,
        partition_enrollment::TransactionPartitionEnrollmentAdmission,
    },
};

use super::{
    aggregate::TransactionSendAggregate,
    input::{
        TransactionSendAdmissionFailure, TransactionSendAdmissionFailureKind,
        TransactionSendRequest,
    },
    model::{
        TransactionSendAccepted, TransactionSendFailure, TransactionSendFailureKind,
        TransactionSendTerminal,
    },
    partitioning::TransactionStickyPartitioners,
    turn::{PendingTransactionSend, TransactionSendSlot},
};

/// One explicit fixed-capacity transactional send and terminal owner.
pub(crate) struct TransactionSendOwner {
    pub(super) compression: CompressionPolicy,
    pub(super) next_send_id: Option<TransactionSendId>,
    pub(super) slot: TransactionSendSlot,
    pub(super) completions: CompletionRegistry<TransactionSendTerminal, TransactionSendPublisher>,
    pub(super) reclaim_pending: Option<CompletionId>,
    pub(super) partitioners: TransactionStickyPartitioners,
}

impl TransactionSendOwner {
    pub(in crate::transaction) fn new(
        compression: CompressionPolicy,
        topic_capacity: usize,
        publisher: TransactionSendPublisher,
    ) -> Self {
        Self {
            compression,
            next_send_id: Some(TransactionSendId::from_raw(1)),
            slot: TransactionSendSlot::Vacant,
            completions: CompletionRegistry::with_publisher(1, publisher),
            reclaim_pending: None,
            partitioners: TransactionStickyPartitioners::new(topic_capacity),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "admission rejection returns the exact caller-owned send request"
    )]
    pub(crate) fn try_send(
        &mut self,
        lifecycle: &mut TransactionLifecycleHost,
        request: TransactionSendRequest,
    ) -> Result<TransactionSendAccepted, TransactionSendAdmissionFailure> {
        self.try_send_with(lifecycle, request)
    }

    #[expect(
        clippy::result_large_err,
        clippy::too_many_lines,
        reason = "atomic admission keeps reservation and linear rollback in one transition"
    )]
    pub(super) fn try_send_with(
        &mut self,
        lifecycle: &mut dyn TransactionSendAggregate,
        request: TransactionSendRequest,
    ) -> Result<TransactionSendAccepted, TransactionSendAdmissionFailure> {
        let request = self.reclaim_for_admission(request)?;
        if !matches!(self.slot, TransactionSendSlot::Vacant) {
            return Err(TransactionSendAdmissionFailure::new(
                TransactionSendAdmissionFailureKind::Busy,
                request,
            ));
        }
        let (completion_id, observer) = match self.completions.reserve() {
            Ok(reservation) => reservation,
            Err(error) => {
                return Err(TransactionSendAdmissionFailure::new(
                    TransactionSendAdmissionFailureKind::Lifecycle(error.into()),
                    request,
                ));
            }
        };
        self.slot = TransactionSendSlot::Reserved(request, completion_id);
        let Some(send_id) = self.next_send_id else {
            return Err(self.rollback(TransactionSendAdmissionFailureKind::SendIdentityExhausted));
        };
        if self.reserved().partition().is_none() || self.reserved().expected_topic_uuid().is_some()
        {
            self.accept_automatic(lifecycle, send_id, completion_id)?;
            return Ok(TransactionSendAccepted::new(send_id, observer));
        }
        let identity = match lifecycle.producer_identity() {
            Ok(identity) => identity,
            Err(error) => {
                return Err(self.rollback(TransactionSendAdmissionFailureKind::Lifecycle(error)));
            }
        };
        let Some(partition_owner) = self.reserved().partition() else {
            return Err(self.rollback(TransactionSendAdmissionFailureKind::InvalidPartition));
        };
        let raw_partition = partition_owner.partition().get();
        let Ok(partition) = i32::try_from(raw_partition) else {
            return Err(self.rollback(TransactionSendAdmissionFailureKind::InvalidPartition));
        };
        let epoch = self.reserved().epoch();
        let sequence = match lifecycle.accept_send(
            epoch,
            send_id,
            partition_owner,
            self.reserved().record_count(),
        ) {
            Ok(sequence) => sequence,
            Err(error) => {
                return Err(self.rollback(TransactionSendAdmissionFailureKind::Lifecycle(error)));
            }
        };
        self.next_send_id = send_id
            .get()
            .checked_add(1)
            .map(TransactionSendId::from_raw);
        let (request, reserved_completion_id) = self.take_reserved();
        debug_assert_eq!(reserved_completion_id, completion_id);
        let expected_topic_uuid = request.expected_topic_uuid();
        let (_, partition_owner, topic, records, max_batch_bytes, deadline) = request.into_parts();
        let batch = TransactionalMaterializationBatch::new(
            topic,
            partition,
            records,
            max_batch_bytes,
            identity,
            sequence,
        );
        let pending = PendingTransactionSend {
            completion_id,
            epoch,
            send_id,
            partition: partition_owner,
            sequence,
            deadline,
            topic_id: partition_owner.topic_id(),
            expected_topic_uuid,
            sticky: false,
            prepared: None,
        };
        match lifecycle.enroll(epoch, batch, deadline) {
            Ok(TransactionPartitionEnrollmentAdmission::Pending) => {
                self.slot = TransactionSendSlot::Enrolling(pending);
            }
            Ok(TransactionPartitionEnrollmentAdmission::Enrolled(fence)) => {
                self.slot = TransactionSendSlot::Ready(pending, fence.into_batch());
            }
            Err(failure) => {
                let kind = failure.kind();
                drop(failure.into_batch());
                lifecycle
                    .settle_unproduced(
                        epoch,
                        send_id,
                        partition_owner,
                        sequence,
                        TransactionSendOutcome::FailedHealthy,
                    )
                    .unwrap_or_else(|_| unreachable!("newly accepted send settles exactly"));
                self.slot = TransactionSendSlot::Terminal(
                    completion_id,
                    TransactionSendTerminal::FailedHealthy {
                        epoch,
                        send_id,
                        failure: TransactionSendFailure::new(
                            TransactionSendFailureKind::Enrollment(kind),
                            DeliveryStatus::NotSent,
                        ),
                    },
                );
            }
        }
        Ok(TransactionSendAccepted::new(send_id, observer))
    }

    pub(super) fn rollback(
        &mut self,
        kind: TransactionSendAdmissionFailureKind,
    ) -> TransactionSendAdmissionFailure {
        let (request, completion_id) = self.take_reserved();
        let kind = match self.completions.rollback_reservation(completion_id) {
            Ok(()) => kind,
            Err(error) => TransactionSendAdmissionFailureKind::Lifecycle(error.into()),
        };
        TransactionSendAdmissionFailure::new(kind, request)
    }

    pub(super) fn reserved(&self) -> &TransactionSendRequest {
        let TransactionSendSlot::Reserved(request, _) = &self.slot else {
            unreachable!("send admission reserves its exact request first");
        };
        request
    }

    pub(super) fn take_reserved(&mut self) -> (TransactionSendRequest, CompletionId) {
        let TransactionSendSlot::Reserved(request, completion_id) =
            core::mem::replace(&mut self.slot, TransactionSendSlot::Vacant)
        else {
            unreachable!("send admission retains its exact reservation");
        };
        (request, completion_id)
    }
}
