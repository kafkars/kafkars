//! Exhaustive public-to-engine finalized-feature request translation.

use crate::{
    DeliveryStatus, ErrorKind, KafkaError,
    admin::{
        FeatureUpdate, FeatureUpdateIntent, UpdateFeaturesRequest, UpdateFeaturesRequestError,
    },
};

use super::engine::{Feature, FeatureIntent, Request};

/// Validates and consumes one inert public request into engine-owned values.
///
/// The shared submit seam must capture the sole public absolute deadline before
/// invoking this translation.
pub(crate) fn translate_request(request: UpdateFeaturesRequest) -> Result<Request, KafkaError> {
    request.validate().map_err(translate_request_error)?;
    let (updates, validate_only) = request.into_parts();
    Ok(Request::new(
        updates.into_iter().map(translate_update).collect(),
        validate_only,
    ))
}

fn translate_update(update: FeatureUpdate) -> Feature {
    let (feature_name, max_version_level, intent) = update.into_parts();
    Feature::new(feature_name, max_version_level, translate_intent(intent))
}

pub(super) const fn translate_intent(intent: FeatureUpdateIntent) -> FeatureIntent {
    match intent {
        FeatureUpdateIntent::Upgrade => FeatureIntent::Upgrade,
        FeatureUpdateIntent::SafeDowngrade => FeatureIntent::SafeDowngrade,
        FeatureUpdateIntent::UnsafeDowngrade => FeatureIntent::UnsafeDowngrade,
    }
}

pub(super) fn translate_request_error(error: UpdateFeaturesRequestError) -> KafkaError {
    KafkaError::new(
        ErrorKind::Configuration,
        format!("UpdateFeatures request validation failed: {error}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}
