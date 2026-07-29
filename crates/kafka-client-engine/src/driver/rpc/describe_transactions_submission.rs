//! Tracked transaction-coordinator submission policy for Admin `DescribeTransactions`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKey, CoordinatorKeyError, CoordinatorKind, RequestOptions, Route,
    RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{DescribeTransactionsRequest, DescribeTransactionsResponse};

use super::super::DriverOwner;

const DESCRIBE_TRANSACTIONS_VERSION: ApiVersion = ApiVersion::new(0);

/// Definitely-unsent failure before the driver accepted request ownership.
#[derive(Debug)]
pub(crate) enum DescribeTransactionsSubmitError {
    InvalidTransactionalId(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for DescribeTransactionsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTransactionalId(source) => {
                write!(
                    formatter,
                    "invalid DescribeTransactions coordinator key: {source}"
                )
            }
            Self::Driver(source) => {
                write!(formatter, "driver rejected DescribeTransactions: {source}")
            }
        }
    }
}

impl Error for DescribeTransactionsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTransactionalId(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    /// Submits one v0 query to the current coordinator for one transactional ID.
    pub(crate) fn submit_tracked_describe_transactions(
        &self,
        transactional_id: &str,
        request: DescribeTransactionsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeTransactionsResponse>, DescribeTransactionsSubmitError> {
        let route = describe_transactions_route(transactional_id)?;
        self.driver
            .request_tracked_with(route, request, describe_transactions_options(deadline))
            .map_err(DescribeTransactionsSubmitError::Driver)
    }
}

pub(super) fn describe_transactions_route(
    transactional_id: &str,
) -> Result<Route, DescribeTransactionsSubmitError> {
    let key = CoordinatorKey::new(CoordinatorKind::Transaction, transactional_id)
        .map_err(DescribeTransactionsSubmitError::InvalidTransactionalId)?;
    Ok(Route::Coordinator { key })
}

pub(super) const fn describe_transactions_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DESCRIBE_TRANSACTIONS_VERSION)
        .with_maximum_version(DESCRIBE_TRANSACTIONS_VERSION)
}
