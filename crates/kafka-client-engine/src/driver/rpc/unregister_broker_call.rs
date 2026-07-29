//! Linear ownership of one accepted tracked controller broker unregistration.

use std::time::Instant;

use kafka_client_core::UnregisterBrokerPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::UnregisterBrokerResponse;

use crate::protocol::admin::unregister_broker::unregister_broker_request;

use super::{
    super::DriverOwner,
    unregister_broker_terminal::{
        RecoveredUnregisterBrokerCall, UnregisterBrokerRawTerminal,
        retain_unregister_broker_terminal,
    },
};

/// One accepted tracked driver call retained beside its deterministic owner.
#[must_use = "an accepted UnregisterBroker call must be terminally settled"]
pub(crate) struct UnregisterBrokerCall {
    call: Option<RoutedCall<UnregisterBrokerResponse>>,
    plan: Option<UnregisterBrokerPlan>,
}

impl UnregisterBrokerCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: UnregisterBrokerPlan,
        deadline: Instant,
    ) -> Result<Self, UnregisterBrokerCallAdmissionFailure> {
        let request = unregister_broker_request(plan.broker_id())
            .map_err(|_source| UnregisterBrokerCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_unregister_broker(request, deadline)
            .map_err(|_source| UnregisterBrokerCallAdmissionFailure::Submit)?;
        Ok(Self {
            call: Some(call),
            plan: Some(plan),
        })
    }

    /// Extracts one ready raw terminal without releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<UnregisterBrokerRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let plan = self.plan.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_unregister_broker_terminal(
                    selected_version,
                    result,
                    route_token,
                    plan,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredUnregisterBrokerCall> {
        let Self { call, plan } = self;
        match (call, plan) {
            (Some(call), Some(plan)) => {
                drop(call);
                Some(RecoveredUnregisterBrokerCall::new(plan))
            }
            _ => None,
        }
    }
}

/// Definitely-unsent request-validation or bounded-driver rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "a rejected UnregisterBroker call must become deterministic input"]
pub(crate) enum UnregisterBrokerCallAdmissionFailure {
    Request,
    Submit,
}
