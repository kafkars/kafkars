//! Linear ownership of one accepted tracked `AnyBroker` configuration-resource query.

use std::time::Instant;

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::{ListConfigResourcesRequest, ListConfigResourcesResponse};

use super::{
    super::DriverOwner,
    list_config_resources_terminal::{
        ListConfigResourcesRawTerminal, RecoveredListConfigResourcesCall,
        retain_list_config_resources_terminal,
    },
};

/// One accepted tracked driver call retained beside its concrete admin owner.
#[must_use = "an accepted ListConfigResources call must be terminally settled"]
pub(crate) struct ListConfigResourcesCall {
    call: Option<RoutedCall<ListConfigResourcesResponse>>,
}

impl ListConfigResourcesCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        request: ListConfigResourcesRequest,
        deadline: Instant,
    ) -> Result<Self, ListConfigResourcesCallAdmissionFailure> {
        let call = driver
            .submit_tracked_list_config_resources(request, deadline)
            .map_err(|_source| ListConfigResourcesCallAdmissionFailure)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<ListConfigResourcesRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_list_config_resources_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredListConfigResourcesCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredListConfigResourcesCall::new()
        })
    }
}

/// Definitely-unsent bounded-driver rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "a rejected ListConfigResources call must become operation input"]
pub(crate) struct ListConfigResourcesCallAdmissionFailure;
