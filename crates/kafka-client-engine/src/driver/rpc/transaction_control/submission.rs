//! Exact-v3 tracked transaction-coordinator submission.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKey, CoordinatorKeyError, CoordinatorKind, RequestOptions, Route,
    RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{EndTxnRequest, EndTxnResponse};

use super::super::super::DriverOwner;

const TRANSACTION_CONTROL_VERSION: ApiVersion = ApiVersion::new(3);

/// Definitely-unsent transaction-coordinator submission failure.
#[derive(Debug)]
pub(crate) enum TransactionControlSubmitError {
    InvalidTransactionalId(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for TransactionControlSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTransactionalId(source) => {
                write!(formatter, "invalid transaction coordinator key: {source}")
            }
            Self::Driver(source) => {
                write!(formatter, "driver rejected transaction control: {source}")
            }
        }
    }
}

impl Error for TransactionControlSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTransactionalId(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_transaction_end(
        &self,
        transactional_id: &str,
        request: EndTxnRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<EndTxnResponse>, TransactionControlSubmitError> {
        let route = transaction_control_route(transactional_id)
            .map_err(TransactionControlSubmitError::InvalidTransactionalId)?;
        self.driver
            .request_tracked_with(route, request, transaction_control_options(deadline))
            .map_err(TransactionControlSubmitError::Driver)
    }
}

pub(in crate::driver::rpc) fn transaction_control_route(
    transactional_id: &str,
) -> Result<Route, CoordinatorKeyError> {
    let key = CoordinatorKey::new(CoordinatorKind::Transaction, transactional_id)?;
    Ok(Route::Coordinator { key })
}

pub(super) const fn transaction_control_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(TRANSACTION_CONTROL_VERSION)
        .with_maximum_version(TRANSACTION_CONTROL_VERSION)
}
