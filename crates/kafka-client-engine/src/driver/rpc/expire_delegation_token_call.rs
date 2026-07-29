//! Linear ownership of one accepted tracked AnyBroker token expiration.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::ExpireDelegationTokenResponse;

use crate::protocol::admin::expire_delegation_token::PreparedExpireDelegationTokenRequest;

use super::{
    super::DriverOwner,
    expire_delegation_token_submission::ExpireDelegationTokenSubmitError,
    expire_delegation_token_terminal::{
        ExpireDelegationTokenRawTerminal, RecoveredExpireDelegationTokenCall,
        retain_expire_delegation_token_terminal,
    },
};

/// One accepted API-key 40 call retained beside its deterministic owner.
#[must_use = "an accepted ExpireDelegationToken call must be terminally settled"]
pub(crate) struct ExpireDelegationTokenCall {
    call: Option<RoutedCall<ExpireDelegationTokenResponse>>,
}

impl ExpireDelegationTokenCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        request: PreparedExpireDelegationTokenRequest,
        deadline: Instant,
    ) -> Result<Self, ExpireDelegationTokenCallAdmissionFailure> {
        let call = driver
            .submit_tracked_expire_delegation_token(request, deadline)
            .map_err(ExpireDelegationTokenCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<ExpireDelegationTokenRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_expire_delegation_token_terminal(
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
    ) -> Option<RecoveredExpireDelegationTokenCall> {
        self.call.take().map(|call| {
            drop(call);
            RecoveredExpireDelegationTokenCall
        })
    }
}

/// Definitely-unsent rejection before tracked driver ownership.
#[derive(Debug)]
pub(crate) enum ExpireDelegationTokenCallAdmissionFailure {
    Driver(ExpireDelegationTokenSubmitError),
}

impl fmt::Display for ExpireDelegationTokenCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for ExpireDelegationTokenCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Driver(source) => Some(source),
        }
    }
}
