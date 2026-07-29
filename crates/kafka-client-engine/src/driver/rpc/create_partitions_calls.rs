//! Bounded ownership and normalization of tracked `CreatePartitions` calls.

use std::{error::Error, fmt};

use kafka_client_core::{CreatePartitionsInput, OperationId};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::CreatePartitionsResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::create_partitions::{
        CreatePartitionsRequestError, create_partitions_request, remaining_timeout_ms,
    },
};

use super::{
    super::DriverOwner,
    create_partitions_refresh::{
        SettledCreatePartitionsCall, response_requires_controller_refresh,
    },
    create_partitions_submission::CreatePartitionsSubmitError,
    create_partitions_terminal::normalize_terminal,
};

struct TrackedCreatePartitionsCall {
    operation_id: OperationId,
    plan: kafka_client_core::CreatePartitionsPlan,
    retained_bytes: usize,
    call: RoutedCall<CreatePartitionsResponse>,
}

pub(crate) struct CreatePartitionsCallPermit<'a> {
    calls: &'a mut Vec<TrackedCreatePartitionsCall>,
}

impl CreatePartitionsCallPermit<'_> {
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        operation_id: OperationId,
        deadline: OperationDeadline,
        plan: kafka_client_core::CreatePartitionsPlan,
        retained_bytes: usize,
        now: kafka_client_core::Moment,
    ) -> Result<(), CreatePartitionsAdmissionFailure> {
        let timeout_ms = remaining_timeout_ms(now, deadline.core())?;
        let request = create_partitions_request(&plan, timeout_ms)?;
        let call = driver.submit_tracked_create_partitions(request, deadline.transport())?;
        self.calls.push(TrackedCreatePartitionsCall {
            operation_id,
            plan,
            retained_bytes,
            call,
        });
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct CreatePartitionsCompletionFailure {
    operation_id: OperationId,
    source: Option<CompletionError>,
}

impl fmt::Display for CreatePartitionsCompletionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            Some(source) => write!(
                formatter,
                "tracked CreatePartitions completion failed for operation {}: {}",
                self.operation_id.get(),
                source
            ),
            None => write!(
                formatter,
                "tracked CreatePartitions result exceeded its reservation for operation {}",
                self.operation_id.get()
            ),
        }
    }
}

impl Error for CreatePartitionsCompletionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

#[derive(Debug)]
pub(crate) enum CreatePartitionsAdmissionFailure {
    Request(CreatePartitionsRequestError),
    Driver,
}

impl CreatePartitionsAdmissionFailure {
    pub(crate) const fn core_input(&self) -> CreatePartitionsInput {
        match self {
            Self::Request(CreatePartitionsRequestError::DeadlineElapsed) => {
                CreatePartitionsInput::DeadlineElapsed
            }
            Self::Request(CreatePartitionsRequestError::NegativeTimeout) | Self::Driver => {
                CreatePartitionsInput::DriverRejected
            }
        }
    }
}

impl From<CreatePartitionsRequestError> for CreatePartitionsAdmissionFailure {
    fn from(error: CreatePartitionsRequestError) -> Self {
        Self::Request(error)
    }
}

impl From<CreatePartitionsSubmitError> for CreatePartitionsAdmissionFailure {
    fn from(_error: CreatePartitionsSubmitError) -> Self {
        Self::Driver
    }
}

pub(crate) struct TrackedCreatePartitionsCalls {
    capacity: usize,
    calls: Vec<TrackedCreatePartitionsCall>,
    settled: Option<SettledCreatePartitionsCall>,
}

impl TrackedCreatePartitionsCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: None,
        }
    }

    pub(crate) fn try_reserve(&mut self) -> Option<CreatePartitionsCallPermit<'_>> {
        if self.retained_count() >= self.capacity {
            return None;
        }
        Some(CreatePartitionsCallPermit {
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
    ) -> Result<Option<&mut SettledCreatePartitionsCall>, CreatePartitionsCompletionFailure> {
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
        let outcome = result.map_err(|source| CreatePartitionsCompletionFailure {
            operation_id: call.operation_id,
            source: Some(source),
        })?;
        let (result, _selected_version, route_token) = outcome.into_parts();
        let broker_requires_controller_refresh = response_requires_controller_refresh(&result);
        let input = normalize_terminal(&call.plan, call.retained_bytes, result).map_err(
            |_retained_accounting| CreatePartitionsCompletionFailure {
                operation_id: call.operation_id,
                source: None,
            },
        )?;
        self.settled = Some(SettledCreatePartitionsCall::new(
            call.operation_id,
            input,
            route_token,
            broker_requires_controller_refresh,
        ));
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
