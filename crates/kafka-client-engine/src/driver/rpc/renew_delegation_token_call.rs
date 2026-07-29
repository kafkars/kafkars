//! Linear ownership of one accepted tracked AnyBroker token renewal.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::RenewDelegationTokenResponse;

use crate::protocol::admin::renew_delegation_token::PreparedRenewDelegationTokenRequest;

use super::{
    super::DriverOwner,
    renew_delegation_token_submission::RenewDelegationTokenSubmitError,
    renew_delegation_token_terminal::{
        RecoveredRenewDelegationTokenCall, RenewDelegationTokenRawTerminal,
        retain_renew_delegation_token_terminal,
    },
};

/// One accepted API-key 39 call retained beside its deterministic owner.
#[must_use = "an accepted RenewDelegationToken call must be terminally settled"]
pub(crate) struct RenewDelegationTokenCall {
    call: Option<RoutedCall<RenewDelegationTokenResponse>>,
}

impl RenewDelegationTokenCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        request: PreparedRenewDelegationTokenRequest,
        deadline: Instant,
    ) -> Result<Self, RenewDelegationTokenCallAdmissionFailure> {
        let call = driver
            .submit_tracked_renew_delegation_token(request, deadline)
            .map_err(RenewDelegationTokenCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<RenewDelegationTokenRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_renew_delegation_token_terminal(
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
        mut self,
    ) -> Option<RecoveredRenewDelegationTokenCall> {
        self.call.take().map(|call| {
            drop(call);
            RecoveredRenewDelegationTokenCall
        })
    }
}

/// Definitely-unsent rejection before tracked driver ownership.
#[derive(Debug)]
pub(crate) enum RenewDelegationTokenCallAdmissionFailure {
    Driver(RenewDelegationTokenSubmitError),
}

impl fmt::Display for RenewDelegationTokenCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for RenewDelegationTokenCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Driver(source) => Some(source),
        }
    }
}
