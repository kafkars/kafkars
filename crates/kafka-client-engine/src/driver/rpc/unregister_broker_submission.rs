//! Bounded controller submission policy for Admin `UnregisterBroker`.

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
    /// Submits one broker unregistration with explicit route-failure settlement.
    pub(crate) fn submit_tracked_unregister_broker(
        &self,
        request: UnregisterBrokerRequest,
        deadline: Instant,
        controller_retry: bool,
    ) -> Result<RoutedCall<UnregisterBrokerResponse>, UnregisterBrokerSubmitError> {
        self.driver
            .request_tracked_with(
                unregister_broker_route(),
                request,
                unregister_broker_options(deadline, controller_retry),
            )
            .map_err(|source| UnregisterBrokerSubmitError { source })
    }
}

pub(super) const fn unregister_broker_route() -> Route {
    Route::Controller
}

pub(super) const fn unregister_broker_options(
    deadline: Instant,
    controller_retry: bool,
) -> RequestOptions {
    let options = RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(UNREGISTER_BROKER_VERSION)
        .with_maximum_version(UNREGISTER_BROKER_VERSION);
    if controller_retry {
        options
    } else {
        options.with_route_failure_rejection()
    }
}
