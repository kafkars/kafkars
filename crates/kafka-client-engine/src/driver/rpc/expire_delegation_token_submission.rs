//! Single-attempt `AnyBroker` policy for Admin `ExpireDelegationToken`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::ExpireDelegationTokenResponse;

use crate::protocol::admin::expire_delegation_token::PreparedExpireDelegationTokenRequest;

use super::super::DriverOwner;

const EXPIRE_DELEGATION_TOKEN_MIN_VERSION: ApiVersion = ApiVersion::new(1);
const EXPIRE_DELEGATION_TOKEN_MAX_VERSION: ApiVersion = ApiVersion::new(2);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct ExpireDelegationTokenSubmitError {
    source: SubmitError,
}

impl fmt::Display for ExpireDelegationTokenSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected ExpireDelegationToken request: {}",
            self.source
        )
    }
}

impl Error for ExpireDelegationTokenSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one token mutation without retry or route invalidation.
    pub(crate) fn submit_tracked_expire_delegation_token(
        &self,
        request: PreparedExpireDelegationTokenRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<ExpireDelegationTokenResponse>, ExpireDelegationTokenSubmitError> {
        self.driver
            .request_tracked_with(
                expire_delegation_token_route(),
                request,
                expire_delegation_token_options(deadline),
            )
            .map_err(|source| ExpireDelegationTokenSubmitError { source })
    }
}

pub(super) const fn expire_delegation_token_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn expire_delegation_token_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(EXPIRE_DELEGATION_TOKEN_MIN_VERSION)
        .with_maximum_version(EXPIRE_DELEGATION_TOKEN_MAX_VERSION)
}
