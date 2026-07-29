//! Single-attempt controller submission policy for Admin `UnregisterBroker`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{UnregisterBrokerRequest, UnregisterBrokerResponse};

use super::super::DriverOwner;

const UNREGISTER_BROKER_VERSION: ApiVersion = ApiVersion::new(0);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct UnregisterBrokerSubmitError {
    source: SubmitError,
}

impl fmt::Display for UnregisterBrokerSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected UnregisterBroker request: {}",
            self.source
        )
    }
}

impl Error for UnregisterBrokerSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one broker unregistration without automatic replay policy.
    pub(crate) fn submit_tracked_unregister_broker(
        &self,
        request: UnregisterBrokerRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<UnregisterBrokerResponse>, UnregisterBrokerSubmitError> {
        self.driver
            .request_tracked_with(
                unregister_broker_route(),
                request,
                unregister_broker_options(deadline),
            )
            .map_err(|source| UnregisterBrokerSubmitError { source })
    }
}

pub(super) const fn unregister_broker_route() -> Route {
    Route::Controller
}

pub(super) const fn unregister_broker_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(UNREGISTER_BROKER_VERSION)
        .with_maximum_version(UNREGISTER_BROKER_VERSION)
}
