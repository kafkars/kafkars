//! Any-broker submission policy for Admin `DescribeUserScramCredentials`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse};

use super::super::DriverOwner;

const DESCRIBE_USER_SCRAM_CREDENTIALS_MIN_VERSION: ApiVersion = ApiVersion::new(0);
const DESCRIBE_USER_SCRAM_CREDENTIALS_MAX_VERSION: ApiVersion = ApiVersion::new(0);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct DescribeUserScramCredentialsSubmitError {
    source: SubmitError,
}

impl fmt::Display for DescribeUserScramCredentialsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected DescribeUserScramCredentials request: {}",
            self.source
        )
    }
}

impl Error for DescribeUserScramCredentialsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one read-only SCRAM credential query through an arbitrary broker.
    pub(crate) fn submit_describe_user_scram_credentials(
        &self,
        request: DescribeUserScramCredentialsRequest,
        deadline: Instant,
    ) -> Result<
        RoutedCall<DescribeUserScramCredentialsResponse>,
        DescribeUserScramCredentialsSubmitError,
    > {
        self.driver
            .request_tracked_with(
                describe_user_scram_credentials_route(),
                request,
                describe_user_scram_credentials_options(deadline),
            )
            .map_err(|source| DescribeUserScramCredentialsSubmitError { source })
    }
}

pub(super) const fn describe_user_scram_credentials_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn describe_user_scram_credentials_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DESCRIBE_USER_SCRAM_CREDENTIALS_MIN_VERSION)
        .with_maximum_version(DESCRIBE_USER_SCRAM_CREDENTIALS_MAX_VERSION)
}
