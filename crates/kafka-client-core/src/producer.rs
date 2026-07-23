//! Atomic producer admission, retained capacity, and terminal settlement.

use core::fmt;
use std::collections::BTreeMap;

use crate::{
    AdmissionRejection, Admitted, ByteBudget, ByteCount, CapacityError, CompletionLedger,
    CompletionLedgerError, Deadline, OperationId, ProducerCompletion, ProducerOperation,
    TerminalEffects, TransitionError, TryAdmitError,
};

/// Rejected transition after producer admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerMachineError {
    /// The operation is not retained by this producer.
    UnknownOperation,
    /// The requested lifecycle transition is invalid.
    Transition(TransitionError),
    /// Terminal-completion ownership rejected the transition.
    Completion(CompletionLedgerError),
    /// Retained-byte accounting rejected the transition.
    Capacity(CapacityError),
}

impl fmt::Display for ProducerMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownOperation => formatter.write_str("producer operation is unknown"),
            Self::Transition(error) => error.fmt(formatter),
            Self::Completion(error) => error.fmt(formatter),
            Self::Capacity(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ProducerMachineError {}

/// Single-owner deterministic producer admission and completion machine.
#[derive(Debug)]
pub struct ProducerMachine {
    admission_open: bool,
    next_operation_id: Option<OperationId>,
    byte_budget: ByteBudget,
    completions: CompletionLedger<ProducerCompletion>,
    operations: BTreeMap<OperationId, ProducerOperation>,
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
        now: Deadline,
        deadline: Deadline,
        bytes: ByteCount,
        value: T,
    ) -> Result<Admitted<T>, TryAdmitError<T>> {
        if !self.admission_open {
            return Err(rejected(AdmissionRejection::Closed, value));
        }
        if deadline <= now {
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

    /// Transfers an accumulated operation into driver ownership.
    pub fn mark_submitted(&mut self, id: OperationId) -> Result<(), ProducerMachineError> {
        let operation = self
            .operations
            .get_mut(&id)
            .ok_or(ProducerMachineError::UnknownOperation)?;
        operation
            .mark_submitted()
            .map_err(ProducerMachineError::Transition)
    }

    /// Retains one terminal result and releases the operation's byte budget.
    pub fn settle(
        &mut self,
        id: OperationId,
        completion: ProducerCompletion,
    ) -> Result<(), ProducerMachineError> {
        let mut next_operation = self
            .operations
            .get(&id)
            .copied()
            .ok_or(ProducerMachineError::UnknownOperation)?;
        let effects = match completion {
            ProducerCompletion::Delivered => next_operation.complete_delivered(),
            ProducerCompletion::Failed(delivery) => next_operation.complete_failed(delivery),
        }
        .map_err(ProducerMachineError::Transition)?;

        self.retain_terminal(id, next_operation, effects)
    }

    /// Expires accepted work before it enters driver ownership.
    pub fn expire_before_submission(
        &mut self,
        id: OperationId,
    ) -> Result<(), ProducerMachineError> {
        let mut next_operation = self
            .operations
            .get(&id)
            .copied()
            .ok_or(ProducerMachineError::UnknownOperation)?;
        let effects = next_operation
            .expire()
            .map_err(ProducerMachineError::Transition)?;

        self.retain_terminal(id, next_operation, effects)
    }

    fn retain_terminal(
        &mut self,
        id: OperationId,
        next_operation: ProducerOperation,
        effects: TerminalEffects,
    ) -> Result<(), ProducerMachineError> {
        let mut next_budget = self.byte_budget;
        if let Some(bytes) = effects.released_bytes() {
            next_budget
                .release(bytes)
                .map_err(ProducerMachineError::Capacity)?;
        }

        self.completions
            .complete(id, effects.completion())
            .map_err(ProducerMachineError::Completion)?;
        self.byte_budget = next_budget;
        self.operations.insert(id, next_operation);
        Ok(())
    }

    /// Observes a terminal result and releases its completion slot.
    pub fn take_completion(
        &mut self,
        id: OperationId,
    ) -> Result<ProducerCompletion, ProducerMachineError> {
        let completion = self
            .completions
            .take(id)
            .map_err(ProducerMachineError::Completion)?;
        let removed = self.operations.remove(&id);
        debug_assert!(removed.is_some());
        Ok(completion)
    }

    /// Permanently stops new admissions without discarding accepted work.
    pub const fn close_admission(&mut self) {
        self.admission_open = false;
    }
}

fn rejected<T>(reason: AdmissionRejection, value: T) -> TryAdmitError<T> {
    TryAdmitError { reason, value }
}
