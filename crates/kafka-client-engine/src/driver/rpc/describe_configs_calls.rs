//! Bounded ownership and normalization of tracked generic `DescribeConfigs` calls.

use std::{error::Error, fmt};

use kafka_client_core::{
    DescribeConfigsInput, DescribeConfigsPlan, DescribeConfigsRoute, OperationId,
};
use kafka_driver::{CompletionError, RouteFailureToken, RoutedCall};
use kafka_wire::DescribeConfigsResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::describe_configs::{DescribeConfigsQuery, describe_configs_request},
};

use super::{
    super::DriverOwner, describe_configs_submission::DescribeConfigsSubmitError,
    describe_configs_terminal::normalize_terminal,
};

struct DescribeConfigsCall {
    operation_id: OperationId,
    plan: DescribeConfigsPlan,
    result_limit: usize,
    call: RoutedCall<DescribeConfigsResponse>,
}

pub(crate) struct DescribeConfigsCallPermit<'a> {
    calls: &'a mut Vec<DescribeConfigsCall>,
}

impl DescribeConfigsCallPermit<'_> {
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        operation_id: OperationId,
        deadline: OperationDeadline,
        route: DescribeConfigsRoute,
        plan: DescribeConfigsPlan,
        result_limit: usize,
    ) -> Result<(), DescribeConfigsAdmissionFailure> {
        let key_storage = plan
            .resources()
            .iter()
            .map(|resource| {
                resource
                    .configuration_keys()
                    .map(|keys| keys.iter().map(String::as_str).collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        let queries = plan
            .resources()
            .iter()
            .zip(&key_storage)
            .map(|(resource, keys)| DescribeConfigsQuery {
                resource_type: resource.resource_type(),
                resource_name: resource.resource_name(),
                configuration_keys: keys.as_deref(),
            })
            .collect::<Vec<_>>();
        let request = describe_configs_request(
            &queries,
            plan.include_synonyms(),
            plan.include_documentation(),
        );
        let call = driver.submit_tracked_describe_configs(request, route, deadline.transport())?;
        self.calls.push(DescribeConfigsCall {
            operation_id,
            plan,
            result_limit,
            call,
        });
        Ok(())
    }
}

pub(crate) struct SettledDescribeConfigsCall {
    operation_id: OperationId,
    input: Option<DescribeConfigsInput>,
    route_token: Option<RouteFailureToken>,
}

impl SettledDescribeConfigsCall {
    pub(crate) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(crate) fn take_input(&mut self) -> Option<DescribeConfigsInput> {
        self.input.take()
    }

    fn discard(self) {
        drop(self.route_token);
    }
}

#[derive(Debug)]
pub(crate) struct DescribeConfigsCompletionFailure {
    operation_id: OperationId,
    source: CompletionError,
}

impl fmt::Display for DescribeConfigsCompletionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "tracked DescribeConfigs completion failed for operation {}: {}",
            self.operation_id.get(),
            self.source
        )
    }
}

impl Error for DescribeConfigsCompletionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug)]
pub(crate) enum DescribeConfigsAdmissionFailure {
    Driver,
}

impl DescribeConfigsAdmissionFailure {
    pub(crate) const fn into_core_input(self) -> DescribeConfigsInput {
        match self {
            Self::Driver => DescribeConfigsInput::DriverRejected,
        }
    }
}

impl From<DescribeConfigsSubmitError> for DescribeConfigsAdmissionFailure {
    fn from(_error: DescribeConfigsSubmitError) -> Self {
        Self::Driver
    }
}

pub(crate) struct DescribeConfigsCalls {
    capacity: usize,
    calls: Vec<DescribeConfigsCall>,
    settled: Option<SettledDescribeConfigsCall>,
}

impl DescribeConfigsCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: None,
        }
    }

    pub(crate) fn try_reserve(&mut self) -> Option<DescribeConfigsCallPermit<'_>> {
        if self.retained_count() >= self.capacity {
            return None;
        }
        Some(DescribeConfigsCallPermit {
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
    ) -> Result<Option<&mut SettledDescribeConfigsCall>, DescribeConfigsCompletionFailure> {
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
        let outcome = result.map_err(|source| DescribeConfigsCompletionFailure {
            operation_id: call.operation_id,
            source,
        })?;
        let (result, selected_version, route_token) = outcome.into_parts();
        let input = normalize_terminal(&call.plan, call.result_limit, selected_version, result);
        self.settled = Some(SettledDescribeConfigsCall {
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
