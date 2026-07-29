//! Bounded ownership of client-metrics resource machines and concrete calls.

mod admission;
mod model;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    ListClientMetricsResourcesEffect, ListClientMetricsResourcesInput,
    ListClientMetricsResourcesMachine, ListClientMetricsResourcesTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminListClientMetricsResourcesPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{ListClientMetricsResourcesCall, ListClientMetricsResourcesRawTerminal},
};

use super::{ListClientMetricsResourcesHostError, ListClientMetricsResourcesObserver};

use model::ListClientMetricsResourcesHandoff;
pub(crate) use model::{ListClientMetricsResourcesSubmission, ListClientMetricsResourcesTurn};

pub(crate) const LIST_CLIENT_METRICS_RESOURCES_CAPACITY: usize = 16;
pub(crate) const LIST_CLIENT_METRICS_RESOURCES_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct ListClientMetricsResourcesAdmission {
    pub(crate) observer: ListClientMetricsResourcesObserver,
    pub(crate) fault: Option<ListClientMetricsResourcesHostError>,
}

struct ListClientMetricsResourcesOperation {
    operation_id: OperationId,
    machine: ListClientMetricsResourcesMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<ListClientMetricsResourcesSubmission>,
    handoff: ListClientMetricsResourcesHandoff,
    call: Option<ListClientMetricsResourcesCall>,
    raw_terminal: Option<ListClientMetricsResourcesRawTerminal>,
    terminal: Option<ListClientMetricsResourcesTerminal>,
}

pub(crate) struct ListClientMetricsResourcesHost {
    operations: Vec<ListClientMetricsResourcesOperation>,
    completions: CompletionRegistry<
        ListClientMetricsResourcesTerminal,
        AdminListClientMetricsResourcesPublisher,
    >,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<ListClientMetricsResourcesHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl ListClientMetricsResourcesHost {
    pub(crate) fn new(publisher: AdminListClientMetricsResourcesPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(LIST_CLIENT_METRICS_RESOURCES_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                LIST_CLIENT_METRICS_RESOURCES_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(LIST_CLIENT_METRICS_RESOURCES_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<ListClientMetricsResourcesTurn, ListClientMetricsResourcesHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(ListClientMetricsResourcesTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(ListClientMetricsResourcesTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(
                operation_id,
                ListClientMetricsResourcesInput::DeadlineElapsed,
            )?;
            return Ok(ListClientMetricsResourcesTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(ListClientMetricsResourcesHostError::MissingSubmission)?;
        self.operations[index].handoff = ListClientMetricsResourcesHandoff::HandedOff;
        Ok(ListClientMetricsResourcesTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: ListClientMetricsResourcesCall,
    ) -> Result<(), ListClientMetricsResourcesHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListClientMetricsResourcesHostError::UnknownOperation)?;
        if self.operations[index].handoff != ListClientMetricsResourcesHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(ListClientMetricsResourcesHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(
            operation_id,
            ListClientMetricsResourcesInput::DriverAccepted,
        )
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), ListClientMetricsResourcesHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListClientMetricsResourcesHostError::UnknownOperation)?;
        if self.operations[index].handoff != ListClientMetricsResourcesHandoff::HandedOff {
            return Err(ListClientMetricsResourcesHostError::InvalidHandoff);
        }
        self.apply(
            operation_id,
            ListClientMetricsResourcesInput::DriverRejected,
        )
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
        input: ListClientMetricsResourcesInput,
    ) -> Result<(), ListClientMetricsResourcesHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListClientMetricsResourcesHostError::UnknownOperation)?;
        let accepted = matches!(&input, ListClientMetricsResourcesInput::DriverAccepted);
        if accepted
            && self.operations[index].handoff != ListClientMetricsResourcesHandoff::HandedOff
        {
            return Err(ListClientMetricsResourcesHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = ListClientMetricsResourcesHandoff::Submitted;
        }
        if let Some(ListClientMetricsResourcesEffect::Complete { terminal, .. }) =
            transition.into_effect()
        {
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
