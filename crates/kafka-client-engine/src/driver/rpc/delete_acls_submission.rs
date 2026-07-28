//! Tracked AnyBroker submission policy for Admin `DeleteAcls`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{DeleteAclsRequest, DeleteAclsResponse};

use crate::protocol::admin::delete_acls::{DELETE_ACLS_MAX_VERSION, DELETE_ACLS_MIN_VERSION};

use super::super::DriverOwner;

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct DeleteAclsSubmitError {
    source: SubmitError,
}

impl fmt::Display for DeleteAclsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected DeleteAcls request: {}",
            self.source
        )
    }
}

impl Error for DeleteAclsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one destructive ACL-deletion batch through an arbitrary broker.
    pub(crate) fn submit_delete_acls(
        &self,
        request: DeleteAclsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DeleteAclsResponse>, DeleteAclsSubmitError> {
        self.driver
            .request_tracked_with(delete_acls_route(), request, delete_acls_options(deadline))
            .map_err(|source| DeleteAclsSubmitError { source })
    }
}

pub(super) const fn delete_acls_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn delete_acls_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ApiVersion::new(DELETE_ACLS_MIN_VERSION))
        .with_maximum_version(ApiVersion::new(DELETE_ACLS_MAX_VERSION))
}
