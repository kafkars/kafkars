//! Linear ownership of one accepted tracked AnyBroker token query.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::DescribeDelegationTokensPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DescribeDelegationTokenResponse;

use crate::protocol::admin::describe_delegation_tokens::PreparedDescribeDelegationTokensRequest;

use super::{
    super::DriverOwner,
    describe_delegation_tokens_submission::DescribeDelegationTokensSubmitError,
    describe_delegation_tokens_terminal::{
        DescribeDelegationTokensRawTerminal, RecoveredDescribeDelegationTokensCall,
        retain_describe_delegation_tokens_terminal,
    },
};

/// One accepted API-key 41 call retained beside its deterministic owner.
#[must_use = "an accepted DescribeDelegationTokens call must be terminally settled"]
pub(crate) struct DescribeDelegationTokensCall {
    call: Option<RoutedCall<DescribeDelegationTokenResponse>>,
}

impl DescribeDelegationTokensCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        request: PreparedDescribeDelegationTokensRequest,
        deadline: Instant,
    ) -> Result<Self, DescribeDelegationTokensCallAdmissionFailure> {
        let call = driver
            .submit_tracked_describe_delegation_tokens(request, deadline)
            .map_err(DescribeDelegationTokensCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeDelegationTokensRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_delegation_tokens_terminal(
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
        plan: DescribeDelegationTokensPlan,
    ) -> Option<RecoveredDescribeDelegationTokensCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredDescribeDelegationTokensCall::new(plan)
        })
    }
}

/// Definitely-unsent rejection before tracked driver ownership.
#[derive(Debug)]
pub(crate) enum DescribeDelegationTokensCallAdmissionFailure {
    Driver(DescribeDelegationTokensSubmitError),
}

impl fmt::Display for DescribeDelegationTokensCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for DescribeDelegationTokensCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Driver(source) => Some(source),
        }
    }
}
