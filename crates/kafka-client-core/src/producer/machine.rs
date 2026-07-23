//! Producer admission owner and its bounded state registries.

use std::collections::BTreeMap;

use crate::{
    AdmissionRejection, Admitted, ByteBudget, ByteCount, CapacityError, CompletionLedger,
    CompletionLedgerError, Deadline, ExplicitRecord, Moment, OperationId, ProducerMachineError,
    ProducerOperation, TryAdmitError,
};

/// Single-owner deterministic producer admission and completion machine.
#[derive(Debug)]
pub struct ProducerMachine {
    pub(super) admission_open: bool,
    pub(super) next_operation_id: Option<OperationId>,
    pub(super) byte_budget: ByteBudget,
    pub(super) completions: CompletionLedger,
    pub(super) operations: BTreeMap<OperationId, ProducerOperation>,
    pub(super) records: BTreeMap<OperationId, ExplicitRecord>,
}

impl ProducerMachine {
    /// Creates an open producer with hard byte and operation-count limits.
    pub const fn new(retained_bytes: ByteCount, completion_capacity: usize) -> Self {
        Self {
            admission_open: true,
            next_operation_id: Some(OperationId::from_raw(1)),
            byte_budget: ByteBudget::new(retained_bytes),
            completions: CompletionLedger::new(completion_capacity),
            operations: BTreeMap::new(),
            records: BTreeMap::new(),
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

    /// Atomically reserves retained bytes and terminal-completion capacity.
    ///
    /// Rejection returns the original value and leaves every capacity owner
    /// unchanged.
    pub fn try_admit<T>(
        &mut self,
        now: Moment,
        deadline: Deadline,
        bytes: ByteCount,
        value: T,
    ) -> Result<Admitted<T>, TryAdmitError<T>> {
        if !self.admission_open {
            return Err(rejected(AdmissionRejection::Closed, value));
        }
        if deadline.is_elapsed_at(now) {
            return Err(rejected(AdmissionRejection::DeadlineElapsed, value));
        }
        let Some(id) = self.next_operation_id else {
            return Err(rejected(AdmissionRejection::IdentityExhausted, value));
        };

        if let Err(error) = self.byte_budget.try_reserve(bytes) {
            let reason = match error {
                CapacityError::Exhausted | CapacityError::OverRelease => {
                    AdmissionRejection::ByteCapacity
                }
                CapacityError::Overflow => AdmissionRejection::ByteCountOverflow,
            };
            return Err(rejected(reason, value));
        }

        if let Err(error) = self.completions.reserve(id) {
            let rollback = self.byte_budget.release(bytes);
            debug_assert_eq!(rollback, Ok(()));
            let reason = match error {
                CompletionLedgerError::Full => AdmissionRejection::CompletionCapacity,
                CompletionLedgerError::DuplicateOperation
                | CompletionLedgerError::UnknownOperation
                | CompletionLedgerError::AlreadyCompleted
                | CompletionLedgerError::NotReady => AdmissionRejection::IdentityExhausted,
            };
            return Err(rejected(reason, value));
        }

        self.operations
            .insert(id, ProducerOperation::admitted(id, deadline, bytes));
        self.next_operation_id = id.get().checked_add(1).map(OperationId::from_raw);
        Ok(Admitted {
            id,
            deadline,
            bytes,
            value,
        })
    }

    pub(crate) fn admit_explicit(
        &mut self,
        now: Moment,
        deadline: Deadline,
        record: ExplicitRecord,
    ) -> Result<OperationId, AdmissionRejection> {
        let admission = self.try_admit(now, deadline, record.retained_bytes(), record);
        let admitted = admission.map_err(|error| error.reason())?;
        let (id, record) = admitted.into_parts();
        self.records.insert(id, record);
        Ok(id)
    }

    pub(crate) fn record(&self, id: OperationId) -> Result<ExplicitRecord, ProducerMachineError> {
        self.records
            .get(&id)
            .copied()
            .ok_or(ProducerMachineError::UnknownOperation)
    }

    pub(crate) fn operation(
        &self,
        id: OperationId,
    ) -> Result<&ProducerOperation, ProducerMachineError> {
        self.operations
            .get(&id)
            .ok_or(ProducerMachineError::UnknownOperation)
    }

    /// Permanently stops new admissions without discarding accepted work.
    pub const fn close_admission(&mut self) {
        self.admission_open = false;
    }
}

fn rejected<T>(reason: AdmissionRejection, value: T) -> TryAdmitError<T> {
    TryAdmitError { reason, value }
}
