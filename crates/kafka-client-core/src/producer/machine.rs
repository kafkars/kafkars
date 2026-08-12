//! Producer admission owner and bounded operation and batch registries.

use std::collections::BTreeMap;

use crate::{
    AdmissionRejection, BatchId, ByteBudget, ByteCount, CapacityError, CompletionLedger,
    CompletionLedgerError, Deadline, ExplicitRecord, Moment, OperationId, ProducerBatchPolicy,
    ProducerOperation, ProducerRetryPolicy,
    id_hash::{IdMap, id_map},
    producer_transition_effect_capacity,
};

use super::{BatchRoute, FlushLedger, ProducerBatch, idempotence::IdempotentProducer};

pub(super) type ProducerOperations = IdMap<OperationId, ProducerOperation>;
type RecordFacts = IdMap<OperationId, ExplicitRecord>;

/// Single-owner deterministic producer admission and completion machine.
#[derive(Debug)]
pub struct ProducerMachine {
    pub(crate) admission_open: bool,
    pub(crate) next_operation_id: Option<OperationId>,
    pub(crate) next_batch_id: Option<BatchId>,
    pub(crate) batch_policy: ProducerBatchPolicy,
    pub(crate) retry_policy: ProducerRetryPolicy,
    pub(crate) compression: crate::CompressionPolicy,
    pub(crate) idempotence: IdempotentProducer,
    pub(crate) byte_budget: ByteBudget,
    pub(crate) completions: CompletionLedger,
    pub(crate) flushes: FlushLedger,
    pub(crate) transition_effect_capacity: Option<usize>,
    pub(crate) operations: ProducerOperations,
    pub(crate) records: RecordFacts,
    pub(crate) open_batches: BTreeMap<BatchRoute, BatchId>,
    pub(crate) batches: BTreeMap<BatchId, ProducerBatch>,
}

impl ProducerMachine {
    /// Creates an open producer whose first slice submits one accumulated record.
    pub const fn new(retained_bytes: ByteCount, completion_capacity: usize) -> Self {
        Self::with_batch_policy_and_flush_capacity(
            retained_bytes,
            completion_capacity,
            ProducerBatchPolicy::single_record(),
            completion_capacity,
        )
    }

    /// Creates an open producer with explicit deterministic batching policy.
    pub const fn with_batch_policy(
        retained_bytes: ByteCount,
        completion_capacity: usize,
        batch_policy: ProducerBatchPolicy,
    ) -> Self {
        Self::with_batch_policy_and_flush_capacity(
            retained_bytes,
            completion_capacity,
            batch_policy,
            completion_capacity,
        )
    }

    /// Creates an open producer with explicit batching and retry policies.
    pub const fn with_batch_and_retry_policy(
        retained_bytes: ByteCount,
        completion_capacity: usize,
        batch_policy: ProducerBatchPolicy,
        retry_policy: ProducerRetryPolicy,
    ) -> Self {
        Self::with_policies_and_flush_capacity(
            retained_bytes,
            completion_capacity,
            batch_policy,
            retry_policy,
            crate::CompressionPolicy::None,
            completion_capacity,
        )
    }

    /// Creates an open producer with explicit batching, retry, and compression policy.
    pub const fn with_batch_retry_and_compression_policy(
        retained_bytes: ByteCount,
        completion_capacity: usize,
        batch_policy: ProducerBatchPolicy,
        retry_policy: ProducerRetryPolicy,
        compression: crate::CompressionPolicy,
    ) -> Self {
        Self::with_policies_and_flush_capacity(
            retained_bytes,
            completion_capacity,
            batch_policy,
            retry_policy,
            compression,
            completion_capacity,
        )
    }

    /// Creates an open producer with independent bounded flush capacity.
    pub const fn with_batch_policy_and_flush_capacity(
        retained_bytes: ByteCount,
        completion_capacity: usize,
        batch_policy: ProducerBatchPolicy,
        flush_capacity: usize,
    ) -> Self {
        Self::with_policies_and_flush_capacity(
            retained_bytes,
            completion_capacity,
            batch_policy,
            ProducerRetryPolicy::none(),
            crate::CompressionPolicy::None,
            flush_capacity,
        )
    }

    /// Creates an open producer with all deterministic policies and capacities.
    pub const fn with_policies_and_flush_capacity(
        retained_bytes: ByteCount,
        completion_capacity: usize,
        batch_policy: ProducerBatchPolicy,
        retry_policy: ProducerRetryPolicy,
        compression: crate::CompressionPolicy,
        flush_capacity: usize,
    ) -> Self {
        Self {
            admission_open: true,
            next_operation_id: Some(OperationId::from_raw(1)),
            next_batch_id: Some(BatchId::from_raw(1)),
            batch_policy,
            retry_policy,
            compression,
            idempotence: IdempotentProducer::new(completion_capacity),
            byte_budget: ByteBudget::new(retained_bytes),
            completions: CompletionLedger::new(completion_capacity),
            flushes: FlushLedger::new(flush_capacity),
            transition_effect_capacity: producer_transition_effect_capacity(
                completion_capacity,
                flush_capacity,
            ),
            operations: id_map(),
            records: id_map(),
            open_batches: BTreeMap::new(),
            batches: BTreeMap::new(),
        }
    }

    /// Returns bytes retained by admitted operations.
    pub const fn retained_bytes(&self) -> ByteCount {
        self.byte_budget.used()
    }

    /// Returns completion slots reserved by active or completed operations.
    pub fn completion_slots(&self) -> usize {
        self.completions.len()
    }

    /// Returns retained flush slots, including terminal results not reclaimed.
    pub fn flush_slots(&self) -> usize {
        self.flushes.len()
    }

    /// Returns the checked maximum effects emitted by one public transition.
    pub const fn transition_effect_capacity(&self) -> Option<usize> {
        self.transition_effect_capacity
    }

    /// Returns whether new producer work may be admitted.
    pub const fn admission_is_open(&self) -> bool {
        self.admission_open
    }

    pub(super) fn reserve_explicit(
        &mut self,
        now: Moment,
        deadline: Deadline,
        record: ExplicitRecord,
        batch_id: BatchId,
    ) -> Result<OperationId, AdmissionRejection> {
        if !self.admission_open {
            return Err(AdmissionRejection::Closed);
        }
        if deadline.is_elapsed_at(now) {
            return Err(AdmissionRejection::DeadlineElapsed);
        }
        let Some(id) = self.next_operation_id else {
            return Err(AdmissionRejection::IdentityExhausted);
        };
        self.reserve_capacity(id, record.retained_bytes())?;
        self.operations.insert(
            id,
            ProducerOperation::admitted(id, deadline, record.retained_bytes(), batch_id),
        );
        self.records.insert(id, record);
        self.next_operation_id = id.get().checked_add(1).map(OperationId::from_raw);
        Ok(id)
    }

    fn reserve_capacity(
        &mut self,
        id: OperationId,
        bytes: ByteCount,
    ) -> Result<(), AdmissionRejection> {
        if let Err(error) = self.byte_budget.try_reserve(bytes) {
            return Err(match error {
                CapacityError::Exhausted | CapacityError::OverRelease => {
                    AdmissionRejection::ByteCapacity
                }
                CapacityError::Overflow => AdmissionRejection::ByteCountOverflow,
            });
        }
        if let Err(error) = self.completions.reserve(id) {
            let rollback = self.byte_budget.release(bytes);
            debug_assert_eq!(rollback, Ok(()));
            return Err(match error {
                CompletionLedgerError::Full => AdmissionRejection::CompletionCapacity,
                CompletionLedgerError::DuplicateOperation
                | CompletionLedgerError::UnknownOperation
                | CompletionLedgerError::AlreadyCompleted
                | CompletionLedgerError::NotReady => AdmissionRejection::IdentityExhausted,
            });
        }
        Ok(())
    }

    pub(crate) fn record(&self, id: OperationId) -> Option<ExplicitRecord> {
        self.records.get(&id).copied()
    }

    pub(crate) fn operation(&self, id: OperationId) -> Option<&ProducerOperation> {
        self.operations.get(&id)
    }

    /// Permanently stops new admissions without discarding accepted work.
    pub const fn close_admission(&mut self) {
        self.admission_open = false;
    }
}
