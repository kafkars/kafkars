//! Any-broker submission policy for Admin `AlterClientQuotas`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{AlterClientQuotasRequest, AlterClientQuotasResponse};

use super::super::DriverOwner;

const ALTER_CLIENT_QUOTAS_MIN_VERSION: ApiVersion = ApiVersion::new(0);
const ALTER_CLIENT_QUOTAS_MAX_VERSION: ApiVersion = ApiVersion::new(1);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct AlterClientQuotasSubmitError {
    source: SubmitError,
}

impl fmt::Display for AlterClientQuotasSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected AlterClientQuotas request: {}",
            self.source
        )
    }
}

impl Error for AlterClientQuotasSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one destructive client-quota batch through an arbitrary broker.
    pub(crate) fn submit_alter_client_quotas(
        &self,
        request: AlterClientQuotasRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<AlterClientQuotasResponse>, AlterClientQuotasSubmitError> {
        self.driver
            .request_tracked_with(
                alter_client_quotas_route(),
                request,
                alter_client_quotas_options(deadline),
            )
            .map_err(|source| AlterClientQuotasSubmitError { source })
    }
}

pub(super) const fn alter_client_quotas_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn alter_client_quotas_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ALTER_CLIENT_QUOTAS_MIN_VERSION)
        .with_maximum_version(ALTER_CLIENT_QUOTAS_MAX_VERSION)
}
