//! Concrete tracked submission of one generated nontransactional identity request.

use std::time::Instant;

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{InitProducerIdRequest, InitProducerIdResponse};

use super::super::DriverOwner;

const INIT_PRODUCER_ID_MAX_VERSION: ApiVersion = ApiVersion::new(5);

impl DriverOwner {
    /// Submits one identity request without restarting its caller-owned deadline.
    ///
    /// `AnyBroker` intentionally yields no invalidation authority. A future
    /// generation-fenced producer owner must retain at most one accepted call.
    pub(crate) fn submit_tracked_init_producer_id(
        &self,
        request: InitProducerIdRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<InitProducerIdResponse>, SubmitError> {
        self.driver.request_tracked_with(
            Route::AnyBroker,
            request,
            init_producer_id_options(deadline),
        )
    }
}

pub(super) const fn init_producer_id_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Control)
        .with_maximum_version(INIT_PRODUCER_ID_MAX_VERSION)
}
