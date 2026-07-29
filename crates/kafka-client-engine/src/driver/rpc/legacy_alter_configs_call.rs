//! Linear ownership of one accepted legacy resource configuration replacement call.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::LegacyAlterConfigsPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::AlterConfigsResponse;

use crate::protocol::admin::legacy_alter_configs::legacy_alter_configs_request;

use super::{
    super::DriverOwner,
    legacy_alter_configs_submission::LegacyAlterConfigsSubmitError,
    legacy_alter_configs_terminal::{
        LegacyAlterConfigsTerminal, RecoveredLegacyAlterConfigsCall,
        retain_legacy_alter_configs_terminal,
    },
};

/// One accepted destructive call retained beside its concrete operation owner.
#[must_use = "an accepted legacy AlterConfigs call must be terminally settled"]
pub(crate) struct LegacyAlterConfigsCall {
    call: Option<RoutedCall<AlterConfigsResponse>>,
}

impl LegacyAlterConfigsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: &LegacyAlterConfigsPlan,
        deadline: Instant,
    ) -> Result<Self, LegacyAlterConfigsCallAdmissionFailure> {
        let request = legacy_alter_configs_request(plan);
        let call = driver
            .submit_tracked_legacy_alter_configs(request, deadline)
            .map_err(LegacyAlterConfigsCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<LegacyAlterConfigsTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_legacy_alter_configs_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is destroyed.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredLegacyAlterConfigsCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredLegacyAlterConfigsCall::new()
        })
    }
}

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
#[must_use = "a rejected legacy AlterConfigs call must become deterministic input"]
pub(crate) enum LegacyAlterConfigsCallAdmissionFailure {
    Driver(LegacyAlterConfigsSubmitError),
}

impl fmt::Display for LegacyAlterConfigsCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Driver(source) => write!(formatter, "{source}"),
        }
    }
}

impl Error for LegacyAlterConfigsCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Driver(source) => Some(source),
        }
    }
}
