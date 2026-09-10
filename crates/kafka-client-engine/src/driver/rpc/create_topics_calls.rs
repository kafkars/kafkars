//! Bounded ownership and semantic normalization of tracked `CreateTopics` calls.

use std::{error::Error, fmt};

use kafka_client_core::{CreateTopicsInput, Deadline, Moment, OperationId};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::CreateTopicsResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::create_topics::{
        CreateTopicsRequestError, create_topics_request, remaining_timeout_ms,
    },
};

use super::{
    super::DriverOwner,
    create_topics_submission::CreateTopicsSubmitError,
    create_topics_terminal::normalize_terminal,
    create_topics_visibility::{SettledCreateTopicsCall, visibility_targets},
};

struct TrackedCreateTopicsCall {
    operation_id: OperationId,
    plan: kafka_client_core::CreateTopicsPlan,
    retained_bytes: usize,
    call: RoutedCall<CreateTopicsResponse>,
}

pub(crate) struct CreateTopicsCallPermit<'a> {
    calls: &'a mut Vec<TrackedCreateTopicsCall>,
}

impl CreateTopicsCallPermit<'_> {
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        operation_id: OperationId,
        deadline: OperationDeadline,
        plan: kafka_client_core::CreateTopicsPlan,
        retained_bytes: usize,
        now: kafka_client_core::Moment,
    ) -> Result<(), CreateTopicsAdmissionFailure> {
        let timeout_ms = remaining_timeout_ms(now, deadline.core())?;
        let request = create_topics_request(&plan, timeout_ms)?;
        let call = driver.submit_tracked_create_topics(request, deadline.transport())?;
        self.calls.push(TrackedCreateTopicsCall {
            operation_id,
            plan,
            retained_bytes,
            call,
        });
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct CreateTopicsCompletionFailure {
    operation_id: OperationId,
    source: Option<CompletionError>,
}

impl fmt::Display for CreateTopicsCompletionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            Some(source) => write!(
                formatter,
                "tracked CreateTopics completion failed for operation {}: {}",
                self.operation_id.get(),
                source
            ),
            None => write!(
                formatter,
                "tracked CreateTopics result exceeded its admitted reservation for operation {}",
                self.operation_id.get()
            ),
        }
    }
}

impl Error for CreateTopicsCompletionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

#[derive(Debug)]
pub(crate) enum CreateTopicsAdmissionFailure {
    Request(CreateTopicsRequestError),
    Driver,
}

impl CreateTopicsAdmissionFailure {
    pub(crate) const fn core_input(&self) -> CreateTopicsInput {
        match self {
            Self::Request(CreateTopicsRequestError::DeadlineElapsed) => {
                CreateTopicsInput::DeadlineElapsed
            }
            Self::Request(CreateTopicsRequestError::NegativeTimeout) | Self::Driver => {
                CreateTopicsInput::DriverRejected
            }
        }
    }
}

impl From<CreateTopicsRequestError> for CreateTopicsAdmissionFailure {
    fn from(error: CreateTopicsRequestError) -> Self {
        Self::Request(error)
    }
}

impl From<CreateTopicsSubmitError> for CreateTopicsAdmissionFailure {
    fn from(_error: CreateTopicsSubmitError) -> Self {
        Self::Driver
    }
}

pub(crate) struct TrackedCreateTopicsCalls {
    capacity: usize,
    calls: Vec<TrackedCreateTopicsCall>,
    settled: Vec<SettledCreateTopicsCall>,
}

impl TrackedCreateTopicsCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn try_reserve(&mut self) -> Option<CreateTopicsCallPermit<'_>> {
        if self.retained_count() >= self.capacity {
            return None;
        }
        Some(CreateTopicsCallPermit {
            calls: &mut self.calls,
        })
    }

    pub(crate) fn retained_count(&self) -> usize {
        self.calls.len().saturating_add(self.settled.len())
    }

    pub(crate) fn advance_one_visibility(&mut self, driver: &DriverOwner, now: Moment) -> bool {
        for settled in &mut self.settled {
            if settled.poll_visibility(driver, now) {
                return true;
            }
        }
        false
    }

    pub(crate) fn poll_next_ready(
        &mut self,
    ) -> Result<Option<&mut SettledCreateTopicsCall>, CreateTopicsCompletionFailure> {
        if let Some(index) = self.ready_settled_index() {
            return Ok(self.settled.get_mut(index));
        }
        let Some((index, result)) = self
            .calls
            .iter()
            .enumerate()
            .find_map(|(index, call)| call.call.try_result().map(|result| (index, result)))
        else {
            return Ok(None);
        };
        let call = self.calls.remove(index);
        let outcome = result.map_err(|source| CreateTopicsCompletionFailure {
            operation_id: call.operation_id,
            source: Some(source),
        })?;
        let (result, _selected_version, route_token) = outcome.into_parts();
        let input = normalize_terminal(&call.plan, call.retained_bytes, result).map_err(
            |_retained_accounting| CreateTopicsCompletionFailure {
                operation_id: call.operation_id,
                source: None,
            },
        )?;
        let visibility_targets =
            visibility_targets(&call.plan, &input).map_err(|()| CreateTopicsCompletionFailure {
                operation_id: call.operation_id,
                source: None,
            })?;
        self.settled.push(SettledCreateTopicsCall::new(
            call.operation_id,
            input,
            route_token,
            visibility_targets,
        ));
        Ok(self.settled.last_mut())
    }

    pub(crate) fn discard_settled(&mut self, operation_id: OperationId) -> bool {
        let Some(index) = self
            .settled
            .iter()
            .position(|settled| settled.operation_id() == operation_id)
        else {
            return false;
        };
        self.settled.remove(index).discard();
        true
    }

    pub(crate) fn discard_after_driver_shutdown(&mut self) {
        self.calls.clear();
        self.settled.clear();
    }

    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        self.settled
            .iter()
            .filter_map(SettledCreateTopicsCall::next_deadline)
            .min()
    }

    fn ready_settled_index(&self) -> Option<usize> {
        self.settled
            .iter()
            .position(SettledCreateTopicsCall::input_ready)
    }

    #[cfg(test)]
    pub(super) fn retain_settled_for_test(&mut self, settled: SettledCreateTopicsCall) {
        self.settled.push(settled);
    }

    #[cfg(test)]
    pub(super) fn take_ready_input_for_test(&mut self) -> Option<(OperationId, CreateTopicsInput)> {
        let index = self.ready_settled_index()?;
        let settled = self.settled.get_mut(index)?;
        Some((settled.operation_id(), settled.take_input()?))
    }
}
