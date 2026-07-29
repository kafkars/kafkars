//! Any-broker submission policy for Admin `AlterUserScramCredentials`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::AlterUserScramCredentialsResponse;

use crate::protocol::admin::alter_user_scram_credentials::PreparedAlterUserScramCredentialsRequest;

use super::super::DriverOwner;

const ALTER_USER_SCRAM_CREDENTIALS_MIN_VERSION: ApiVersion = ApiVersion::new(0);
const ALTER_USER_SCRAM_CREDENTIALS_MAX_VERSION: ApiVersion = ApiVersion::new(0);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct AlterUserScramCredentialsSubmitError {
    source: SubmitError,
}

impl fmt::Display for AlterUserScramCredentialsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected AlterUserScramCredentials request: {}",
            self.source
        )
    }
}

impl Error for AlterUserScramCredentialsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Consumes one prepared secret-bearing request for a single destructive attempt.
    pub(crate) fn submit_alter_user_scram_credentials(
        &self,
        request: PreparedAlterUserScramCredentialsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<AlterUserScramCredentialsResponse>, AlterUserScramCredentialsSubmitError>
    {
        self.driver
            .request_tracked_with(
                alter_user_scram_credentials_route(),
                request,
                alter_user_scram_credentials_options(deadline),
            )
            .map_err(|source| AlterUserScramCredentialsSubmitError { source })
    }
}

pub(super) const fn alter_user_scram_credentials_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn alter_user_scram_credentials_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ALTER_USER_SCRAM_CREDENTIALS_MIN_VERSION)
        .with_maximum_version(ALTER_USER_SCRAM_CREDENTIALS_MAX_VERSION)
}
