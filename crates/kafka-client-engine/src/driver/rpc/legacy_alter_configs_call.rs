//! Linear ownership of one accepted legacy resource configuration replacement call.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::{LegacyAlterConfigsPlan, LegacyAlterConfigsRoute};
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
    route: Option<LegacyAlterConfigsRoute>,
    plan: Option<LegacyAlterConfigsPlan>,
}

impl LegacyAlterConfigsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        route: LegacyAlterConfigsRoute,
        plan: LegacyAlterConfigsPlan,
        deadline: Instant,
    ) -> Result<Self, LegacyAlterConfigsCallAdmissionFailure> {
        let request = legacy_alter_configs_request(&plan);
        let call = match driver.submit_tracked_legacy_alter_configs(request, route, deadline) {
            Ok(call) => call,
            Err(source) => {
                return Err(LegacyAlterConfigsCallAdmissionFailure {
                    source,
                    route,
                    plan,
                });
            }
        };
        Ok(Self {
            call: Some(call),
            route: Some(route),
            plan: Some(plan),
        })
    }

    /// Extracts one ready raw terminal without releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<LegacyAlterConfigsTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let route = self.route.take()?;
                let plan = self.plan.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_legacy_alter_configs_terminal(
                    selected_version,
                    result,
                    route_token,
                    route,
                    plan,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    pub(crate) fn route(&self) -> LegacyAlterConfigsRoute {
        self.route
            .unwrap_or_else(|| unreachable!("accepted legacy AlterConfigs call retains its route"))
    }

    pub(crate) fn plan(&self) -> &LegacyAlterConfigsPlan {
        self.plan
            .as_ref()
            .unwrap_or_else(|| unreachable!("accepted legacy AlterConfigs call retains its plan"))
    }

    /// Seals unresolved ownership only after the unique driver is destroyed.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredLegacyAlterConfigsCall> {
        let Self { call, route, plan } = self;
        match (call, route, plan) {
            (Some(call), Some(route), Some(plan)) => {
                drop(call);
                Some(RecoveredLegacyAlterConfigsCall::new(route, plan))
            }
            _ => None,
        }
    }
}

/// Definitely-unsent bounded-driver rejection.
#[must_use = "a rejected legacy AlterConfigs call must become deterministic input"]
#[derive(Debug)]
pub(crate) struct LegacyAlterConfigsCallAdmissionFailure {
    source: LegacyAlterConfigsSubmitError,
    route: LegacyAlterConfigsRoute,
    plan: LegacyAlterConfigsPlan,
}

impl LegacyAlterConfigsCallAdmissionFailure {
    pub(crate) fn into_correlation(self) -> (LegacyAlterConfigsRoute, LegacyAlterConfigsPlan) {
        (self.route, self.plan)
    }
}

impl fmt::Display for LegacyAlterConfigsCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.source)
    }
}

impl Error for LegacyAlterConfigsCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
