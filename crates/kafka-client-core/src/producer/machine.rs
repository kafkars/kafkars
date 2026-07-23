//! Producer admission owner and bounded operation and batch registries.

use std::collections::BTreeMap;

use crate::{
    AdmissionRejection, BatchId, ByteBudget, ByteCount, CapacityError, CompletionLedger,
    CompletionLedgerError, Deadline, ExplicitRecord, Moment, OperationId, ProducerBatchPolicy,
    ProducerOperation,
};

use super::{BatchRoute, ProducerBatch};

/// Single-owner deterministic producer admission and completion machine.
#[derive(Debug)]
pub struct ProducerMachine {
    pub(crate) admission_open: bool,
    pub(crate) next_operation_id: Option<OperationId>,
    pub(crate) next_batch_id: Option<BatchId>,
    pub(crate) batch_policy: ProducerBatchPolicy,
    pub(crate) byte_budget: ByteBudget,
    pub(crate) completions: CompletionLedger,
    pub(crate) operations: BTreeMap<OperationId, ProducerOperation>,
    pub(crate) records: BTreeMap<OperationId, ExplicitRecord>,
    pub(crate) open_batches: BTreeMap<BatchRoute, BatchId>,
    pub(crate) batches: BTreeMap<BatchId, ProducerBatch>,
}

impl ProducerMachine {
    /// Creates an open producer whose first slice submits one accumulated record.
    pub const fn new(retained_bytes: ByteCount, completion_capacity: usize) -> Self {
        Self::with_batch_policy(
            retained_bytes,
            completion_capacity,
            ProducerBatchPolicy::single_record(),
        )
    }

    /// Creates an open producer with explicit deterministic batching policy.
    pub const fn with_batch_policy(
        retained_bytes: ByteCount,
        completion_capacity: usize,
        batch_policy: ProducerBatchPolicy,
    ) -> Self {
        Self {
            admission_open: true,
            next_operation_id: Some(OperationId::from_raw(1)),
            next_batch_id: Some(BatchId::from_raw(1)),
            batch_policy,
            byte_budget: ByteBudget::new(retained_bytes),
            completions: CompletionLedger::new(completion_capacity),
            operations: BTreeMap::new(),
            records: BTreeMap::new(),
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
