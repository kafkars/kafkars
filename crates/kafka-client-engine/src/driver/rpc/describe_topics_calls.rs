//! Bounded ownership and normalization of plain transient Metadata calls.

use std::{error::Error, fmt};

use kafka_client_core::{DescribeTopicsInput, DescribeTopicsPlan, OperationId};
use kafka_driver::{Call, CompletionError, RequestError};
use kafka_wire::MetadataResponse;

use crate::{clock::OperationDeadline, protocol::admin::describe_topics::describe_topics_request};

use super::{
    super::DriverOwner, describe_topics_submission::DescribeTopicsSubmitError,
    describe_topics_terminal::normalize_terminal,
};

struct DescribeTopicsCall {
    operation_id: OperationId,
    plan: DescribeTopicsPlan,
    retained_bytes: usize,
    call: Call<Result<MetadataResponse, RequestError>>,
}

pub(crate) struct DescribeTopicsCallPermit<'a> {
    calls: &'a mut Vec<DescribeTopicsCall>,
}

impl DescribeTopicsCallPermit<'_> {
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        operation_id: OperationId,
        deadline: OperationDeadline,
        plan: DescribeTopicsPlan,
        retained_bytes: usize,
    ) -> Result<(), DescribeTopicsAdmissionFailure> {
        let request = describe_topics_request(plan.topics().iter().map(String::as_str));
        let call = driver.submit_describe_topics(request, deadline.transport())?;
        self.calls.push(DescribeTopicsCall {
            operation_id,
            plan,
            retained_bytes,
            call,
        });
        Ok(())
    }
}

pub(crate) struct SettledDescribeTopicsCall {
    operation_id: OperationId,
    input: Option<DescribeTopicsInput>,
}

impl SettledDescribeTopicsCall {
    pub(crate) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(crate) fn take_input(&mut self) -> Option<DescribeTopicsInput> {
        self.input.take()
    }
}

#[derive(Debug)]
pub(crate) struct DescribeTopicsCompletionFailure {
    operation_id: OperationId,
    source: CompletionError,
}

impl fmt::Display for DescribeTopicsCompletionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeTopics completion failed for operation {}: {}",
            self.operation_id.get(),
            self.source
        )
    }
}

impl Error for DescribeTopicsCompletionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug)]
pub(crate) struct DescribeTopicsAdmissionFailure;

impl DescribeTopicsAdmissionFailure {
    pub(crate) const fn into_core_input(self) -> DescribeTopicsInput {
        let Self = self;
        DescribeTopicsInput::DriverRejected
    }
}

impl From<DescribeTopicsSubmitError> for DescribeTopicsAdmissionFailure {
    fn from(_error: DescribeTopicsSubmitError) -> Self {
        Self
    }
}

pub(crate) struct DescribeTopicsCalls {
    capacity: usize,
    calls: Vec<DescribeTopicsCall>,
    settled: Option<SettledDescribeTopicsCall>,
}

impl DescribeTopicsCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: None,
        }
    }

    pub(crate) fn try_reserve(&mut self) -> Option<DescribeTopicsCallPermit<'_>> {
        if self.retained_count() >= self.capacity {
            return None;
        }
        Some(DescribeTopicsCallPermit {
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
    ) -> Result<Option<&mut SettledDescribeTopicsCall>, DescribeTopicsCompletionFailure> {
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
        let result = result.map_err(|source| DescribeTopicsCompletionFailure {
            operation_id: call.operation_id,
            source,
        })?;
        let input = normalize_terminal(&call.plan, call.retained_bytes, result);
        self.settled = Some(SettledDescribeTopicsCall {
            operation_id: call.operation_id,
            input: Some(input),
        });
        Ok(self.settled.as_mut())
    }

    pub(crate) fn discard_settled(&mut self) {
        drop(self.settled.take());
    }

    pub(crate) fn discard_after_driver_shutdown(&mut self) {
        self.calls.clear();
        self.discard_settled();
    }
}
