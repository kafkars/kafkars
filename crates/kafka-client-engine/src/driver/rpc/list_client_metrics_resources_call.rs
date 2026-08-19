//! Linear ownership of one accepted tracked `AnyBroker` client-metrics resource query.

use std::time::Instant;

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::ListConfigResourcesResponse;

use crate::protocol::admin::list_client_metrics_resources::list_client_metrics_resources_request;

use super::{
    super::DriverOwner,
    list_client_metrics_resources_terminal::{
        ListClientMetricsResourcesRawTerminal, RecoveredListClientMetricsResourcesCall,
        retain_list_client_metrics_resources_terminal,
    },
};

/// One accepted tracked driver call retained beside its concrete admin owner.
#[must_use = "an accepted ListClientMetricsResources call must be terminally settled"]
pub(crate) struct ListClientMetricsResourcesCall {
    call: Option<RoutedCall<ListConfigResourcesResponse>>,
}

impl ListClientMetricsResourcesCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        deadline: Instant,
    ) -> Result<Self, ListClientMetricsResourcesCallAdmissionFailure> {
        let request = list_client_metrics_resources_request();
        let call = driver
            .submit_tracked_list_client_metrics_resources(request, deadline)
            .map_err(|_source| ListClientMetricsResourcesCallAdmissionFailure)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without releasing its route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<ListClientMetricsResourcesRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_list_client_metrics_resources_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Option<RecoveredListClientMetricsResourcesCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredListClientMetricsResourcesCall::new()
        })
    }
}

/// Definitely-unsent bounded-driver rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "a rejected ListClientMetricsResources call must become operation input"]
pub(crate) struct ListClientMetricsResourcesCallAdmissionFailure;
