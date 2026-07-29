//! Bounded ownership and normalization of tracked `IncrementalAlterConfigs` calls.

use std::{error::Error, fmt};

use kafka_client_core::{IncrementalAlterConfigsInput, IncrementalAlterConfigsPlan, OperationId};
use kafka_driver::{CompletionError, RouteFailureToken, RoutedCall};
use kafka_wire::IncrementalAlterConfigsResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::incremental_alter_configs::incremental_alter_configs_request,
};

use super::{
    super::DriverOwner, incremental_alter_configs_submission::IncrementalAlterConfigsSubmitError,
    incremental_alter_configs_terminal::normalize_terminal,
};

struct IncrementalAlterConfigsCall {
    operation_id: OperationId,
    plan: IncrementalAlterConfigsPlan,
    result_limit: usize,
    call: RoutedCall<IncrementalAlterConfigsResponse>,
}

pub(crate) struct IncrementalAlterConfigsCallPermit<'a> {
    calls: &'a mut Vec<IncrementalAlterConfigsCall>,
}

impl IncrementalAlterConfigsCallPermit<'_> {
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        operation_id: OperationId,
        deadline: OperationDeadline,
        plan: IncrementalAlterConfigsPlan,
        result_limit: usize,
    ) -> Result<(), IncrementalAlterConfigsAdmissionFailure> {
        let request = incremental_alter_configs_request(&plan);
        let call =
            driver.submit_tracked_incremental_alter_configs(request, deadline.transport())?;
        self.calls.push(IncrementalAlterConfigsCall {
            operation_id,
            plan,
            result_limit,
            call,
        });
        Ok(())
    }
}

pub(crate) struct SettledIncrementalAlterConfigsCall {
    operation_id: OperationId,
    input: Option<IncrementalAlterConfigsInput>,
    route_token: Option<RouteFailureToken>,
}

impl SettledIncrementalAlterConfigsCall {
    pub(crate) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(crate) fn take_input(&mut self) -> Option<IncrementalAlterConfigsInput> {
        self.input.take()
    }

    fn discard(self) {
        drop(self.route_token);
    }
}

#[derive(Debug)]
pub(crate) struct IncrementalAlterConfigsCompletionFailure {
    operation_id: OperationId,
    source: CompletionError,
}

impl fmt::Display for IncrementalAlterConfigsCompletionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "tracked IncrementalAlterConfigs completion failed for operation {}: {}",
            self.operation_id.get(),
            self.source
        )
    }
}

impl Error for IncrementalAlterConfigsCompletionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug)]
pub(crate) enum IncrementalAlterConfigsAdmissionFailure {
    Driver,
}

impl IncrementalAlterConfigsAdmissionFailure {
    pub(crate) const fn into_core_input(self) -> IncrementalAlterConfigsInput {
        match self {
            Self::Driver => IncrementalAlterConfigsInput::DriverRejected,
        }
    }
}

impl From<IncrementalAlterConfigsSubmitError> for IncrementalAlterConfigsAdmissionFailure {
    fn from(_error: IncrementalAlterConfigsSubmitError) -> Self {
        Self::Driver
    }
}

pub(crate) struct IncrementalAlterConfigsCalls {
    capacity: usize,
    calls: Vec<IncrementalAlterConfigsCall>,
    settled: Option<SettledIncrementalAlterConfigsCall>,
}

impl IncrementalAlterConfigsCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: None,
        }
    }

    pub(crate) fn try_reserve(&mut self) -> Option<IncrementalAlterConfigsCallPermit<'_>> {
        if self.retained_count() >= self.capacity {
            return None;
        }
        Some(IncrementalAlterConfigsCallPermit {
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
    ) -> Result<
        Option<&mut SettledIncrementalAlterConfigsCall>,
        IncrementalAlterConfigsCompletionFailure,
    > {
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
        let outcome = result.map_err(|source| IncrementalAlterConfigsCompletionFailure {
            operation_id: call.operation_id,
            source,
        })?;
        let (result, selected_version, route_token) = outcome.into_parts();
        let input = normalize_terminal(&call.plan, call.result_limit, selected_version, result);
        self.settled = Some(SettledIncrementalAlterConfigsCall {
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
