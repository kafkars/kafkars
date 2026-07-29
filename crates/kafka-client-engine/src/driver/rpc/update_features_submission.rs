//! Single-attempt controller submission policy for Admin `UpdateFeatures`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::UpdateFeaturesResponse;

use crate::protocol::admin::update_features::{
    PreparedUpdateFeaturesRequest, UPDATE_FEATURES_MAX_VERSION,
};

use super::super::DriverOwner;

const UPDATE_FEATURES_MAXIMUM_VERSION: ApiVersion = ApiVersion::new(UPDATE_FEATURES_MAX_VERSION);
const UPDATE_FEATURES_MAXIMUM_FLOOR: i16 = 1;

/// Definitely-unsent version-floor or driver-admission failure.
#[derive(Debug)]
pub(crate) enum UpdateFeaturesSubmitError {
    InvalidVersionFloor { actual: i16 },
    Driver(SubmitError),
}

impl fmt::Display for UpdateFeaturesSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidVersionFloor { actual } => {
                write!(
                    formatter,
                    "invalid UpdateFeatures API-version floor {actual}"
                )
            }
            Self::Driver(source) => {
                write!(formatter, "driver rejected UpdateFeatures call: {source}")
            }
        }
    }
}

impl Error for UpdateFeaturesSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidVersionFloor { .. } => None,
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    /// Submits one destructive request to the current controller without retry policy.
    pub(crate) fn submit_tracked_update_features(
        &self,
        request: PreparedUpdateFeaturesRequest,
        minimum_version: i16,
        deadline: Instant,
    ) -> Result<RoutedCall<UpdateFeaturesResponse>, UpdateFeaturesSubmitError> {
        self.driver
            .request_tracked_with(
                update_features_route(),
                request,
                update_features_options(deadline, minimum_version)?,
            )
            .map_err(UpdateFeaturesSubmitError::Driver)
    }
}

pub(super) const fn update_features_route() -> Route {
    Route::Controller
}

pub(super) fn update_features_options(
    deadline: Instant,
    minimum_version: i16,
) -> Result<RequestOptions, UpdateFeaturesSubmitError> {
    if !(0..=UPDATE_FEATURES_MAXIMUM_FLOOR).contains(&minimum_version) {
        return Err(UpdateFeaturesSubmitError::InvalidVersionFloor {
            actual: minimum_version,
        });
    }
    Ok(RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ApiVersion::new(minimum_version))
        .with_maximum_version(UPDATE_FEATURES_MAXIMUM_VERSION))
}
