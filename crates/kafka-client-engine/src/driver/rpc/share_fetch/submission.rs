//! Exact-broker API-78 v1 long-poll submission policy.

use std::{error::Error, fmt};

use kafka_client_core::ShareFetchBrokerId;
use kafka_driver::{
    ApiVersion, BrokerId, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{ShareFetchRequest, ShareFetchResponse};

use crate::{
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::consumer::share_fetch::{SHARE_FETCH_MAX_VERSION, SHARE_FETCH_MIN_VERSION},
};

const SHARE_FETCH_MIN: ApiVersion = ApiVersion::new(SHARE_FETCH_MIN_VERSION);
const SHARE_FETCH_MAX: ApiVersion = ApiVersion::new(SHARE_FETCH_MAX_VERSION);

#[derive(Debug)]
pub(super) enum ShareFetchDriverSubmitError {
    InvalidBroker(i32),
    Driver(SubmitError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareFetchDriverSubmitErrorKind {
    Full,
    Terminal,
}

impl ShareFetchDriverSubmitError {
    #[allow(
        clippy::match_same_arms,
        unreachable_patterns,
        reason = "the published driver submit error is non-exhaustive"
    )]
    pub(super) const fn kind(&self) -> ShareFetchDriverSubmitErrorKind {
        match self {
            Self::Driver(SubmitError::Full) => ShareFetchDriverSubmitErrorKind::Full,
            Self::InvalidBroker(_) | Self::Driver(_) => ShareFetchDriverSubmitErrorKind::Terminal,
        }
    }
}

impl fmt::Display for ShareFetchDriverSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBroker(broker_id) => {
                write!(formatter, "invalid ShareFetch broker route {broker_id}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected ShareFetch: {source}"),
        }
    }
}

impl Error for ShareFetchDriverSubmitError {}

impl DriverOwner {
    pub(super) fn submit_tracked_share_fetch(
        &self,
        broker_id: ShareFetchBrokerId,
        request: ShareFetchRequest,
        deadline: OperationDeadline,
    ) -> Result<RoutedCall<ShareFetchResponse>, ShareFetchDriverSubmitError> {
        let route = share_fetch_route(broker_id)?;
        self.driver
            .request_tracked_with(route, request, share_fetch_options(deadline))
            .map_err(ShareFetchDriverSubmitError::Driver)
    }
}

pub(super) fn share_fetch_route(
    broker_id: ShareFetchBrokerId,
) -> Result<Route, ShareFetchDriverSubmitError> {
    let raw = broker_id.get();
    let broker_id =
        BrokerId::new(raw).map_err(|_error| ShareFetchDriverSubmitError::InvalidBroker(raw))?;
    Ok(Route::Broker { broker_id })
}

pub(super) const fn share_fetch_options(deadline: OperationDeadline) -> RequestOptions {
    RequestOptions::new(deadline.transport())
        .with_traffic_class(TrafficClass::LongPoll)
        .with_minimum_version(SHARE_FETCH_MIN)
        .with_maximum_version(SHARE_FETCH_MAX)
}
