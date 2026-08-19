//! Single-attempt `AnyBroker` policy for Admin `DescribeDelegationTokens`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::DescribeDelegationTokenResponse;

use crate::protocol::admin::describe_delegation_tokens::PreparedDescribeDelegationTokensRequest;

use super::super::DriverOwner;

const DESCRIBE_DELEGATION_TOKENS_MIN_VERSION: ApiVersion = ApiVersion::new(1);
const DESCRIBE_DELEGATION_TOKENS_MAX_VERSION: ApiVersion = ApiVersion::new(3);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct DescribeDelegationTokensSubmitError {
    source: SubmitError,
}

impl fmt::Display for DescribeDelegationTokensSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected DescribeDelegationTokens request: {}",
            self.source
        )
    }
}

impl Error for DescribeDelegationTokensSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one read-only query without retry or route invalidation.
    pub(crate) fn submit_tracked_describe_delegation_tokens(
        &self,
        request: PreparedDescribeDelegationTokensRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeDelegationTokenResponse>, DescribeDelegationTokensSubmitError>
    {
        self.driver
            .request_tracked_with(
                describe_delegation_tokens_route(),
                request,
                describe_delegation_tokens_options(deadline),
            )
            .map_err(|source| DescribeDelegationTokensSubmitError { source })
    }
}

pub(super) const fn describe_delegation_tokens_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn describe_delegation_tokens_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DESCRIBE_DELEGATION_TOKENS_MIN_VERSION)
        .with_maximum_version(DESCRIBE_DELEGATION_TOKENS_MAX_VERSION)
}
