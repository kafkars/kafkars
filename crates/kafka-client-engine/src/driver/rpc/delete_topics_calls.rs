//! Bounded ownership and normalization of tracked `DeleteTopics` calls.

use std::{error::Error, fmt};

use kafka_client_core::{DeleteTopicsInput, OperationId};
use kafka_driver::{CompletionError, RouteFailureToken, RoutedCall};
use kafka_wire::DeleteTopicsResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::delete_topics::{
        DeleteTopicsRequestError, delete_topics_request, remaining_timeout_ms,
    },
};

use super::{
    super::DriverOwner, delete_topics_submission::DeleteTopicsSubmitError,
    delete_topics_terminal::normalize_terminal,
};

struct TrackedDeleteTopicsCall {
    operation_id: OperationId,
    plan: kafka_client_core::DeleteTopicsPlan,
    retained_bytes: usize,
    call: RoutedCall<DeleteTopicsResponse>,
}

pub(crate) struct DeleteTopicsCallPermit<'a> {
    calls: &'a mut Vec<TrackedDeleteTopicsCall>,
}

impl DeleteTopicsCallPermit<'_> {
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        operation_id: OperationId,
        deadline: OperationDeadline,
        plan: kafka_client_core::DeleteTopicsPlan,
        retained_bytes: usize,
        now: kafka_client_core::Moment,
    ) -> Result<(), DeleteTopicsAdmissionFailure> {
        let timeout_ms = remaining_timeout_ms(now, deadline.core())?;
        let request = delete_topics_request(&plan, timeout_ms)?;
        let call = if plan.topic_ids().is_empty() {
            driver.submit_tracked_delete_topics(request, deadline.transport())?
        } else {
            driver.submit_tracked_delete_topics_by_id(request, deadline.transport())?
        };
        self.calls.push(TrackedDeleteTopicsCall {
            operation_id,
            plan,
            retained_bytes,
            call,
        });
        Ok(())
    }
}

pub(crate) struct SettledDeleteTopicsCall {
    operation_id: OperationId,
    input: Option<DeleteTopicsInput>,
    route_token: Option<RouteFailureToken>,
}

impl SettledDeleteTopicsCall {
    pub(crate) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(crate) fn take_input(&mut self) -> Option<DeleteTopicsInput> {
        self.input.take()
    }

    fn discard(self) {
        drop(self.route_token);
    }
}

#[derive(Debug)]
pub(crate) struct DeleteTopicsCompletionFailure {
    operation_id: OperationId,
    source: Option<CompletionError>,
}

impl fmt::Display for DeleteTopicsCompletionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            Some(source) => write!(
                formatter,
                "tracked DeleteTopics completion failed for operation {}: {}",
                self.operation_id.get(),
                source
            ),
            None => write!(
                formatter,
                "tracked DeleteTopics result exceeded its reservation for operation {}",
                self.operation_id.get()
            ),
        }
    }
}

impl Error for DeleteTopicsCompletionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

#[derive(Debug)]
pub(crate) enum DeleteTopicsAdmissionFailure {
    Request(DeleteTopicsRequestError),
    Driver,
}

impl DeleteTopicsAdmissionFailure {
    pub(crate) const fn core_input(&self) -> DeleteTopicsInput {
        match self {
            Self::Request(DeleteTopicsRequestError::DeadlineElapsed) => {
                DeleteTopicsInput::DeadlineElapsed
            }
            Self::Request(DeleteTopicsRequestError::NegativeTimeout) | Self::Driver => {
                DeleteTopicsInput::DriverRejected
            }
        }
    }
}

impl From<DeleteTopicsRequestError> for DeleteTopicsAdmissionFailure {
    fn from(error: DeleteTopicsRequestError) -> Self {
        Self::Request(error)
    }
}

impl From<DeleteTopicsSubmitError> for DeleteTopicsAdmissionFailure {
    fn from(_error: DeleteTopicsSubmitError) -> Self {
        Self::Driver
    }
}

pub(crate) struct TrackedDeleteTopicsCalls {
    capacity: usize,
    calls: Vec<TrackedDeleteTopicsCall>,
    settled: Option<SettledDeleteTopicsCall>,
}

impl TrackedDeleteTopicsCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: None,
        }
    }

    pub(crate) fn try_reserve(&mut self) -> Option<DeleteTopicsCallPermit<'_>> {
        if self.retained_count() >= self.capacity {
            return None;
        }
        Some(DeleteTopicsCallPermit {
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
    ) -> Result<Option<&mut SettledDeleteTopicsCall>, DeleteTopicsCompletionFailure> {
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
        let outcome = result.map_err(|source| DeleteTopicsCompletionFailure {
            operation_id: call.operation_id,
            source: Some(source),
        })?;
        let (result, _selected_version, route_token) = outcome.into_parts();
        let input = normalize_terminal(&call.plan, call.retained_bytes, result).map_err(
            |_retained_accounting| DeleteTopicsCompletionFailure {
                operation_id: call.operation_id,
                source: None,
            },
        )?;
        self.settled = Some(SettledDeleteTopicsCall {
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
