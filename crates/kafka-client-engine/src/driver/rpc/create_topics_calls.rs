//! Bounded ownership and semantic normalization of tracked `CreateTopics` calls.

use std::{error::Error, fmt};

use kafka_client_core::{CreateTopicsInput, OperationId};
use kafka_driver::{CompletionError, RouteFailureToken, RoutedCall};
use kafka_wire::CreateTopicsResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::create_topics::{
        CreateTopicsRequestError, create_topics_request, remaining_timeout_ms,
    },
};

use super::{
    super::DriverOwner, create_topics_submission::CreateTopicsSubmitError,
    create_topics_terminal::normalize_terminal,
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

pub(crate) struct SettledCreateTopicsCall {
    operation_id: OperationId,
    input: Option<CreateTopicsInput>,
    route_token: Option<RouteFailureToken>,
}

impl SettledCreateTopicsCall {
    pub(crate) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(crate) fn take_input(&mut self) -> Option<CreateTopicsInput> {
        self.input.take()
    }

    fn discard(self) {
        drop(self.route_token);
    }

    #[cfg(test)]
    pub(super) fn from_input_for_test(input: CreateTopicsInput) -> Self {
        Self {
            operation_id: OperationId::from_raw(1),
            input: Some(input),
            route_token: None,
        }
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
    settled: Option<SettledCreateTopicsCall>,
}

impl TrackedCreateTopicsCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: None,
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
        self.calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
    }

    pub(crate) fn poll_next_ready(
        &mut self,
    ) -> Result<Option<&mut SettledCreateTopicsCall>, CreateTopicsCompletionFailure> {
        if self.settled.is_some() {
            return Ok(self.settled.as_mut());
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
        self.settled = Some(SettledCreateTopicsCall {
            operation_id: call.operation_id,
            input: Some(input),
            route_token,
        });
        Ok(self.settled.as_mut())
    }

    pub(crate) fn discard_settled(&mut self) {
        if let Some(settled) = self.settled.take() {
            settled.discard();
        }
    }

    pub(crate) fn discard_after_driver_shutdown(&mut self) {
        self.calls.clear();
        self.discard_settled();
    }
}
