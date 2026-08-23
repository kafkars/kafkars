//! Exact-broker API-79 v1 interactive submission policy.

use std::{error::Error, fmt};

use kafka_client_core::ShareFetchBrokerId;
use kafka_driver::{
    ApiVersion, BrokerId, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{ShareAcknowledgeRequest, ShareAcknowledgeResponse};

use crate::{
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::consumer::share_acknowledge::{
        SHARE_ACKNOWLEDGE_MAX_VERSION, SHARE_ACKNOWLEDGE_MIN_VERSION,
    },
};

const SHARE_ACKNOWLEDGE_MIN: ApiVersion = ApiVersion::new(SHARE_ACKNOWLEDGE_MIN_VERSION);
const SHARE_ACKNOWLEDGE_MAX: ApiVersion = ApiVersion::new(SHARE_ACKNOWLEDGE_MAX_VERSION);

#[derive(Debug)]
pub(super) enum ShareAcknowledgeDriverSubmitError {
    InvalidBroker(i32),
    Driver(SubmitError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareAcknowledgeDriverSubmitErrorKind {
    Full,
    Terminal,
}

impl ShareAcknowledgeDriverSubmitError {
    #[allow(
        clippy::match_same_arms,
        unreachable_patterns,
        reason = "the published driver submit error is non-exhaustive"
    )]
    pub(super) const fn kind(&self) -> ShareAcknowledgeDriverSubmitErrorKind {
        match self {
            Self::Driver(SubmitError::Full) => ShareAcknowledgeDriverSubmitErrorKind::Full,
            Self::InvalidBroker(_) | Self::Driver(_) => {
                ShareAcknowledgeDriverSubmitErrorKind::Terminal
            }
        }
    }
}

impl fmt::Display for ShareAcknowledgeDriverSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBroker(broker_id) => {
                write!(
                    formatter,
                    "invalid ShareAcknowledge broker route {broker_id}"
                )
            }
            Self::Driver(source) => write!(formatter, "driver rejected ShareAcknowledge: {source}"),
        }
    }
}

impl Error for ShareAcknowledgeDriverSubmitError {}

impl DriverOwner {
    pub(super) fn submit_tracked_share_acknowledge(
        &self,
        broker_id: ShareFetchBrokerId,
        request: ShareAcknowledgeRequest,
        deadline: OperationDeadline,
    ) -> Result<RoutedCall<ShareAcknowledgeResponse>, ShareAcknowledgeDriverSubmitError> {
        let route = share_acknowledge_route(broker_id)?;
        self.driver
            .request_tracked_with(route, request, share_acknowledge_options(deadline))
            .map_err(ShareAcknowledgeDriverSubmitError::Driver)
    }
}

pub(super) fn share_acknowledge_route(
    broker_id: ShareFetchBrokerId,
) -> Result<Route, ShareAcknowledgeDriverSubmitError> {
    let raw = broker_id.get();
    let broker_id = BrokerId::new(raw)
        .map_err(|_error| ShareAcknowledgeDriverSubmitError::InvalidBroker(raw))?;
    Ok(Route::Broker { broker_id })
}

pub(super) const fn share_acknowledge_options(deadline: OperationDeadline) -> RequestOptions {
    RequestOptions::new(deadline.transport())
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(SHARE_ACKNOWLEDGE_MIN)
        .with_maximum_version(SHARE_ACKNOWLEDGE_MAX)
}
