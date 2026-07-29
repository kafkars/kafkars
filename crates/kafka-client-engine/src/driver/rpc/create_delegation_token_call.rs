//! Linear ownership of one accepted tracked AnyBroker token creation.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::CreateDelegationTokenResponse;

use crate::protocol::admin::create_delegation_token::PreparedCreateDelegationTokenRequest;

use super::{
    super::DriverOwner,
    create_delegation_token_submission::CreateDelegationTokenSubmitError,
    create_delegation_token_terminal::{
        CreateDelegationTokenRawTerminal, RecoveredCreateDelegationTokenCall,
        retain_create_delegation_token_terminal,
    },
};

/// One accepted API-key 38 call retained beside its deterministic owner.
#[must_use = "an accepted CreateDelegationToken call must be terminally settled"]
pub(crate) struct CreateDelegationTokenCall {
    call: Option<RoutedCall<CreateDelegationTokenResponse>>,
}

impl CreateDelegationTokenCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        request: PreparedCreateDelegationTokenRequest,
        deadline: Instant,
    ) -> Result<Self, CreateDelegationTokenCallAdmissionFailure> {
        let call = driver
            .submit_tracked_create_delegation_token(request, deadline)
            .map_err(CreateDelegationTokenCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<CreateDelegationTokenRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_create_delegation_token_terminal(
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
    ) -> Option<RecoveredCreateDelegationTokenCall> {
        self.call.take().map(|call| {
            drop(call);
            RecoveredCreateDelegationTokenCall
        })
    }
}

/// Definitely-unsent rejection before tracked driver ownership.
#[derive(Debug)]
pub(crate) enum CreateDelegationTokenCallAdmissionFailure {
    Driver(CreateDelegationTokenSubmitError),
}

impl fmt::Display for CreateDelegationTokenCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for CreateDelegationTokenCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Driver(source) => Some(source),
        }
    }
}
