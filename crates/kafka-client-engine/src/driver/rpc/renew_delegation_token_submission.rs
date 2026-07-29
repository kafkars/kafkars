//! Single-attempt AnyBroker policy for Admin `RenewDelegationToken`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::RenewDelegationTokenResponse;

use crate::protocol::admin::renew_delegation_token::PreparedRenewDelegationTokenRequest;

use super::super::DriverOwner;

const RENEW_DELEGATION_TOKEN_MIN_VERSION: ApiVersion = ApiVersion::new(1);
const RENEW_DELEGATION_TOKEN_MAX_VERSION: ApiVersion = ApiVersion::new(2);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct RenewDelegationTokenSubmitError {
    source: SubmitError,
}

impl fmt::Display for RenewDelegationTokenSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected RenewDelegationToken request: {}",
            self.source
        )
    }
}

impl Error for RenewDelegationTokenSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one token mutation without retry or route invalidation.
    pub(crate) fn submit_tracked_renew_delegation_token(
        &self,
        request: PreparedRenewDelegationTokenRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<RenewDelegationTokenResponse>, RenewDelegationTokenSubmitError> {
        self.driver
            .request_tracked_with(
                renew_delegation_token_route(),
                request,
                renew_delegation_token_options(deadline),
            )
            .map_err(|source| RenewDelegationTokenSubmitError { source })
    }
}

pub(super) const fn renew_delegation_token_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn renew_delegation_token_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(RENEW_DELEGATION_TOKEN_MIN_VERSION)
        .with_maximum_version(RENEW_DELEGATION_TOKEN_MAX_VERSION)
}
