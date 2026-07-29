//! Single-attempt AnyBroker submission policy for Admin `CreateDelegationToken`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::CreateDelegationTokenResponse;

use crate::protocol::admin::create_delegation_token::PreparedCreateDelegationTokenRequest;

use super::super::DriverOwner;

const CREATE_DELEGATION_TOKEN_MIN_VERSION: ApiVersion = ApiVersion::new(1);
const CREATE_DELEGATION_TOKEN_EXPLICIT_OWNER_MIN_VERSION: ApiVersion = ApiVersion::new(3);
const CREATE_DELEGATION_TOKEN_MAX_VERSION: ApiVersion = ApiVersion::new(3);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct CreateDelegationTokenSubmitError {
    source: SubmitError,
}

impl fmt::Display for CreateDelegationTokenSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected CreateDelegationToken request: {}",
            self.source
        )
    }
}

impl Error for CreateDelegationTokenSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one token mutation without retry or route invalidation.
    pub(crate) fn submit_tracked_create_delegation_token(
        &self,
        request: PreparedCreateDelegationTokenRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<CreateDelegationTokenResponse>, CreateDelegationTokenSubmitError> {
        let minimum_version = create_delegation_token_minimum_version(request.minimum_version());
        self.driver
            .request_tracked_with(
                create_delegation_token_route(),
                request,
                create_delegation_token_options(deadline, minimum_version),
            )
            .map_err(|source| CreateDelegationTokenSubmitError { source })
    }
}

pub(super) const fn create_delegation_token_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn create_delegation_token_minimum_version(requested: i16) -> ApiVersion {
    if requested >= CREATE_DELEGATION_TOKEN_EXPLICIT_OWNER_MIN_VERSION.value() {
        CREATE_DELEGATION_TOKEN_EXPLICIT_OWNER_MIN_VERSION
    } else {
        CREATE_DELEGATION_TOKEN_MIN_VERSION
    }
}

pub(super) const fn create_delegation_token_options(
    deadline: Instant,
    minimum_version: ApiVersion,
) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(minimum_version)
        .with_maximum_version(CREATE_DELEGATION_TOKEN_MAX_VERSION)
}
