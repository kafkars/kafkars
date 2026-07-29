//! Bounded ownership of broker-unregistration machines and concrete calls.

mod admission;
mod model;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    Moment, OperationId, UnregisterBrokerEffect, UnregisterBrokerInput, UnregisterBrokerMachine,
    UnregisterBrokerTerminal,
};

use crate::{
    admin::AdminUnregisterBrokerPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{UnregisterBrokerCall, UnregisterBrokerRawTerminal},
};

use super::{UnregisterBrokerHostError, UnregisterBrokerObserver};

use model::UnregisterBrokerHandoff;
pub(crate) use model::{UnregisterBrokerSubmission, UnregisterBrokerTurn};

pub(crate) const UNREGISTER_BROKER_CAPACITY: usize = 16;
/// Maximum normalized retained result bytes charged per operation.
pub(crate) const UNREGISTER_BROKER_RESULT_BYTES: usize = 4 * 1024;
/// Aggregate bytes retained by active and published broker unregistrations.
pub(crate) const UNREGISTER_BROKER_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct UnregisterBrokerAdmission {
    pub(crate) observer: UnregisterBrokerObserver,
    pub(crate) fault: Option<UnregisterBrokerHostError>,
}

struct UnregisterBrokerOperation {
    operation_id: OperationId,
    machine: UnregisterBrokerMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<UnregisterBrokerSubmission>,
    handoff: UnregisterBrokerHandoff,
    call: Option<UnregisterBrokerCall>,
    raw_terminal: Option<UnregisterBrokerRawTerminal>,
    terminal: Option<UnregisterBrokerTerminal>,
}

pub(crate) struct UnregisterBrokerHost {
    operations: Vec<UnregisterBrokerOperation>,
    completions: CompletionRegistry<UnregisterBrokerTerminal, AdminUnregisterBrokerPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<UnregisterBrokerHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl UnregisterBrokerHost {
    pub(crate) fn new(publisher: AdminUnregisterBrokerPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(UNREGISTER_BROKER_CAPACITY),
            completions: CompletionRegistry::with_publisher(UNREGISTER_BROKER_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(UNREGISTER_BROKER_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<UnregisterBrokerTurn, UnregisterBrokerHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(UnregisterBrokerTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(UnregisterBrokerTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, UnregisterBrokerInput::DeadlineElapsed)?;
            return Ok(UnregisterBrokerTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(UnregisterBrokerHostError::MissingSubmission)?;
        self.operations[index].handoff = UnregisterBrokerHandoff::HandedOff;
        Ok(UnregisterBrokerTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: UnregisterBrokerCall,
    ) -> Result<(), UnregisterBrokerHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(UnregisterBrokerHostError::UnknownOperation)?;
        if self.operations[index].handoff != UnregisterBrokerHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(UnregisterBrokerHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, UnregisterBrokerInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), UnregisterBrokerHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(UnregisterBrokerHostError::UnknownOperation)?;
        if self.operations[index].handoff != UnregisterBrokerHandoff::HandedOff {
            return Err(UnregisterBrokerHostError::InvalidHandoff);
        }
        self.apply(operation_id, UnregisterBrokerInput::DriverRejected)
    }

    pub(crate) fn close_admission(&mut self) {
        self.accepting = false;
    }

    pub(crate) fn unsettled(&self) -> usize {
        self.operations.len()
    }

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.operations
            .iter()
            .filter(|operation| operation.submission.is_some())
            .map(|operation| operation.deadline.core())
            .min()
    }

    fn operation_index(&self, operation_id: OperationId) -> Option<usize> {
        self.operations
            .iter()
            .position(|operation| operation.operation_id == operation_id)
    }

    fn apply(
        &mut self,
        operation_id: OperationId,
        input: UnregisterBrokerInput,
    ) -> Result<(), UnregisterBrokerHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(UnregisterBrokerHostError::UnknownOperation)?;
        let accepted = matches!(&input, UnregisterBrokerInput::DriverAccepted);
        if accepted && self.operations[index].handoff != UnregisterBrokerHandoff::HandedOff {
            return Err(UnregisterBrokerHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = UnregisterBrokerHandoff::Submitted;
        }
        if let Some(UnregisterBrokerEffect::Complete { terminal, .. }) = transition.into_effect() {
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
