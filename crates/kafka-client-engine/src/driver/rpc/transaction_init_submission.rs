//! Tracked transaction-coordinator submission of generated `InitProducerId`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKey, CoordinatorKeyError, CoordinatorKind, RequestOptions, Route,
    RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{InitProducerIdRequest, InitProducerIdResponse};

use super::super::DriverOwner;

const MAXIMUM_VERSION: ApiVersion = ApiVersion::new(5);

#[derive(Debug)]
pub(crate) enum TransactionInitSubmitError {
    InvalidTransactionalId(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for TransactionInitSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTransactionalId(source) => {
                write!(formatter, "invalid transaction coordinator key: {source}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected InitProducerId: {source}"),
        }
    }
}

impl Error for TransactionInitSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTransactionalId(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_transaction_init(
        &self,
        transactional_id: &str,
        request: InitProducerIdRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<InitProducerIdResponse>, TransactionInitSubmitError> {
        let route = transaction_coordinator_route(transactional_id)
            .map_err(TransactionInitSubmitError::InvalidTransactionalId)?;
        self.driver
            .request_tracked_with(route, request, transaction_init_options(deadline))
            .map_err(TransactionInitSubmitError::Driver)
    }
}

pub(super) fn transaction_coordinator_route(
    transactional_id: &str,
) -> Result<Route, CoordinatorKeyError> {
    let key = CoordinatorKey::new(CoordinatorKind::Transaction, transactional_id)?;
    Ok(Route::Coordinator { key })
}

pub(super) const fn transaction_init_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(MAXIMUM_VERSION)
}
