//! Linear ownership of one accepted tracked controller-routed feature mutation.

use std::time::Instant;

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::UpdateFeaturesResponse;

use crate::protocol::admin::update_features::PreparedUpdateFeaturesRequest;

use super::{
    super::DriverOwner,
    update_features_terminal::{
        RecoveredUpdateFeaturesCall, UpdateFeaturesRawTerminal, retain_update_features_terminal,
    },
};

/// One accepted tracked driver call retained beside its deterministic owner.
#[must_use = "an accepted UpdateFeatures call must be terminally settled"]
pub(crate) struct UpdateFeaturesCall {
    call: Option<RoutedCall<UpdateFeaturesResponse>>,
}

impl UpdateFeaturesCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        request: PreparedUpdateFeaturesRequest,
        minimum_version: i16,
        deadline: Instant,
    ) -> Result<Self, UpdateFeaturesCallAdmissionFailure> {
        let call = driver
            .submit_tracked_update_features(request, minimum_version, deadline)
            .map_err(|_source| UpdateFeaturesCallAdmissionFailure)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<UpdateFeaturesRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_update_features_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(mut self) -> Option<RecoveredUpdateFeaturesCall> {
        self.call.take().map(|call| {
            drop(call);
            RecoveredUpdateFeaturesCall
        })
    }
}

/// Definitely-unsent version-floor or bounded-driver rejection.
#[must_use = "a rejected UpdateFeatures call must become deterministic input"]
pub(crate) struct UpdateFeaturesCallAdmissionFailure;
