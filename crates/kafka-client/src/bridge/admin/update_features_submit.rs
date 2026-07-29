//! Capture-first admission handoff for public Admin `UpdateFeatures`.

use std::time::Duration;

use super::AdminEngine;
use crate::{
    DeliveryStatus, ErrorKind, KafkaError,
    admin::UpdateFeaturesRequest,
    bridge::update_features::{AdminUpdateFeatures, translate_request},
};

impl AdminEngine {
    pub(crate) fn submit_update_features(
        &self,
        request: UpdateFeaturesRequest,
        timeout: Duration,
    ) -> AdminUpdateFeatures {
        let capture = match self.handle.capture_update_features(timeout) {
            Ok(capture) => capture,
            Err(error) => return AdminUpdateFeatures::from_admission(Err(error)),
        };
        if timeout.is_zero() {
            return AdminUpdateFeatures::from_request_error(invalid_zero_timeout());
        }
        let request = match translate_request(request) {
            Ok(request) => request,
            Err(error) => return AdminUpdateFeatures::from_request_error(error),
        };
        AdminUpdateFeatures::from_admission(capture.try_submit(request))
    }
}

fn invalid_zero_timeout() -> KafkaError {
    KafkaError::new(
        ErrorKind::Configuration,
        "UpdateFeatures deadline duration must be positive",
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}
