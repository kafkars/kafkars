//! Exact-v4 tracked submission for transactional offset coordination.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, TxnOffsetCommitRequest,
    TxnOffsetCommitResponse,
};

use super::super::{
    super::DriverOwner, group_coordinator_route::group_coordinator_route,
    transaction_control::transaction_control_route,
};

const VERSION: ApiVersion = ApiVersion::new(4);

/// Definitely-unsent route or driver admission failure.
#[derive(Debug)]
pub(crate) enum TransactionOffsetSubmitError {
    InvalidTransactionalId(CoordinatorKeyError),
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for TransactionOffsetSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTransactionalId(source) => {
                write!(formatter, "invalid transaction coordinator key: {source}")
            }
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid group coordinator key: {source}")
            }
            Self::Driver(source) => {
                write!(formatter, "driver rejected transaction offsets: {source}")
            }
        }
    }
}

impl Error for TransactionOffsetSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTransactionalId(source) | Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_transaction_add_offsets(
        &self,
        transactional_id: &str,
        request: AddOffsetsToTxnRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<AddOffsetsToTxnResponse>, TransactionOffsetSubmitError> {
        let route = add_offsets_route(transactional_id)
            .map_err(TransactionOffsetSubmitError::InvalidTransactionalId)?;
        self.driver
            .request_tracked_with(route, request, transaction_offset_options(deadline))
            .map_err(TransactionOffsetSubmitError::Driver)
    }

    pub(super) fn submit_tracked_transaction_offset_commit(
        &self,
        group_id: &str,
        request: TxnOffsetCommitRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<TxnOffsetCommitResponse>, TransactionOffsetSubmitError> {
        let route =
            offset_commit_route(group_id).map_err(TransactionOffsetSubmitError::InvalidGroup)?;
        self.driver
            .request_tracked_with(route, request, transaction_offset_options(deadline))
            .map_err(TransactionOffsetSubmitError::Driver)
    }
}

pub(super) fn add_offsets_route(transactional_id: &str) -> Result<Route, CoordinatorKeyError> {
    transaction_control_route(transactional_id)
}

pub(super) fn offset_commit_route(group_id: &str) -> Result<Route, CoordinatorKeyError> {
    group_coordinator_route(group_id)
}

pub(super) const fn transaction_offset_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(VERSION)
        .with_maximum_version(VERSION)
}
