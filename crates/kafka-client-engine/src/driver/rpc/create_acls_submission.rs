//! Tracked AnyBroker submission policy for Admin `CreateAcls`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{CreateAclsRequest, CreateAclsResponse};

use crate::protocol::admin::create_acls::{CREATE_ACLS_MAX_VERSION, CREATE_ACLS_MIN_VERSION};

use super::super::DriverOwner;

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct CreateAclsSubmitError {
    source: SubmitError,
}

impl fmt::Display for CreateAclsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected CreateAcls request: {}",
            self.source
        )
    }
}

impl Error for CreateAclsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one destructive ACL-creation batch through an arbitrary broker.
    pub(crate) fn submit_create_acls(
        &self,
        request: CreateAclsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<CreateAclsResponse>, CreateAclsSubmitError> {
        self.driver
            .request_tracked_with(create_acls_route(), request, create_acls_options(deadline))
            .map_err(|source| CreateAclsSubmitError { source })
    }
}

pub(super) const fn create_acls_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn create_acls_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ApiVersion::new(CREATE_ACLS_MIN_VERSION))
        .with_maximum_version(ApiVersion::new(CREATE_ACLS_MAX_VERSION))
}
