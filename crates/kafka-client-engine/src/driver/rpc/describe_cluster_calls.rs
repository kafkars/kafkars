//! Bounded ownership and normalization of plain `DescribeCluster` calls.

use std::{error::Error, fmt};

use kafka_client_core::{DescribeClusterInput, OperationId};
use kafka_driver::{Call, CompletionError, RequestError};
use kafka_wire::DescribeClusterResponse;

use crate::{
    clock::OperationDeadline, protocol::admin::describe_cluster::describe_cluster_request,
};

use super::{
    super::DriverOwner, describe_cluster_submission::DescribeClusterSubmitError,
    describe_cluster_terminal::normalize_terminal,
};

struct DescribeClusterCall {
    operation_id: OperationId,
    retained_bytes: usize,
    include_fenced_brokers: bool,
    include_authorized_operations: bool,
    call: Call<Result<DescribeClusterResponse, RequestError>>,
}

pub(crate) struct DescribeClusterCallPermit<'a> {
    calls: &'a mut Vec<DescribeClusterCall>,
}

impl DescribeClusterCallPermit<'_> {
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        operation_id: OperationId,
        deadline: OperationDeadline,
        retained_bytes: usize,
        include_fenced_brokers: bool,
        include_authorized_operations: bool,
    ) -> Result<(), DescribeClusterAdmissionFailure> {
        DescribeClusterAdmissionFailure::validate_options(
            include_fenced_brokers,
            include_authorized_operations,
        )?;
        let request =
            describe_cluster_request(include_fenced_brokers, include_authorized_operations);
        let call = driver.submit_describe_cluster(
            request,
            deadline.transport(),
            include_fenced_brokers,
            include_authorized_operations,
        )?;
        self.calls.push(DescribeClusterCall {
            operation_id,
            retained_bytes,
            include_fenced_brokers,
            include_authorized_operations,
            call,
        });
        Ok(())
    }
}

pub(crate) struct SettledDescribeClusterCall {
    operation_id: OperationId,
    input: Option<DescribeClusterInput>,
}

impl SettledDescribeClusterCall {
    pub(crate) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(crate) fn take_input(&mut self) -> Option<DescribeClusterInput> {
        self.input.take()
    }
}

#[derive(Debug)]
pub(crate) struct DescribeClusterCompletionFailure {
    operation_id: OperationId,
    source: CompletionError,
}

impl fmt::Display for DescribeClusterCompletionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeCluster completion failed for operation {}: {}",
            self.operation_id.get(),
            self.source
        )
    }
}

impl Error for DescribeClusterCompletionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug)]
pub(crate) enum DescribeClusterAdmissionFailure {
    DriverRejected,
    Compatibility,
}

impl DescribeClusterAdmissionFailure {
    pub(super) const fn validate_options(
        include_fenced_brokers: bool,
        _include_authorized_operations: bool,
    ) -> Result<(), Self> {
        if include_fenced_brokers {
            Err(Self::Compatibility)
        } else {
            Ok(())
        }
    }

    pub(crate) const fn into_core_input(self) -> DescribeClusterInput {
        match self {
            Self::DriverRejected => DescribeClusterInput::DriverRejected,
            Self::Compatibility => DescribeClusterInput::ProtocolIncompatible {
                delivery: kafka_client_core::DeliveryStatus::NotSent,
            },
        }
    }
}

impl From<DescribeClusterSubmitError> for DescribeClusterAdmissionFailure {
    fn from(_error: DescribeClusterSubmitError) -> Self {
        Self::DriverRejected
    }
}

pub(crate) struct DescribeClusterCalls {
    capacity: usize,
    calls: Vec<DescribeClusterCall>,
    settled: Option<SettledDescribeClusterCall>,
}

impl DescribeClusterCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: None,
        }
    }

    pub(crate) fn try_reserve(&mut self) -> Option<DescribeClusterCallPermit<'_>> {
        if self.retained_count() >= self.capacity {
            return None;
        }
        Some(DescribeClusterCallPermit {
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
    ) -> Result<Option<&mut SettledDescribeClusterCall>, DescribeClusterCompletionFailure> {
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
        let result = result.map_err(|source| DescribeClusterCompletionFailure {
            operation_id: call.operation_id,
            source,
        })?;
        self.settled = Some(SettledDescribeClusterCall {
            operation_id: call.operation_id,
            input: Some(normalize_terminal(
                call.retained_bytes,
                call.include_fenced_brokers,
                call.include_authorized_operations,
                result,
            )),
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
