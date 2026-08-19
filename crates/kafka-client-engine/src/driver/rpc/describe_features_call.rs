//! Linear ownership of one accepted tracked `AnyBroker` feature query.

use std::time::Instant;

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::ApiVersionsResponse;

use crate::protocol::admin::describe_features::describe_features_request;

use super::{
    super::DriverOwner,
    describe_features_terminal::{
        DescribeFeaturesRawTerminal, RecoveredDescribeFeaturesCall,
        retain_describe_features_terminal,
    },
};

/// One accepted tracked driver call retained beside its concrete admin owner.
#[must_use = "an accepted DescribeFeatures call must be terminally settled"]
pub(crate) struct DescribeFeaturesCall {
    call: Option<RoutedCall<ApiVersionsResponse>>,
}

impl DescribeFeaturesCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        deadline: Instant,
    ) -> Result<Self, DescribeFeaturesCallAdmissionFailure> {
        let request = describe_features_request();
        let call = driver
            .submit_tracked_describe_features(request, deadline)
            .map_err(|_source| DescribeFeaturesCallAdmissionFailure)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without releasing its route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeFeaturesRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_features_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredDescribeFeaturesCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredDescribeFeaturesCall::new()
        })
    }
}

/// Definitely-unsent bounded-driver rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "a rejected DescribeFeatures call must become operation input"]
pub(crate) struct DescribeFeaturesCallAdmissionFailure;
