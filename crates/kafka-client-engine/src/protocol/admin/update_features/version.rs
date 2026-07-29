//! Closed API-key 57 version window and feature-derived request floor.

use super::{UpdateFeatureMode, UpdateFeaturesRequestPlan};

/// Newest generated version, whose success response omits per-feature results.
pub(crate) const UPDATE_FEATURES_MAX_VERSION: i16 = 2;
pub(super) const UPDATE_FEATURES_MIN_VERSION: i16 = 0;

pub(super) fn update_features_version_floor(plan: UpdateFeaturesRequestPlan<'_>) -> i16 {
    if plan.validate_only()
        || plan
            .updates()
            .iter()
            .any(|update| update.mode() == UpdateFeatureMode::UnsafeDowngrade)
    {
        1
    } else {
        UPDATE_FEATURES_MIN_VERSION
    }
}

pub(super) const fn supports_update_features_version(version: i16) -> bool {
    version >= UPDATE_FEATURES_MIN_VERSION && version <= UPDATE_FEATURES_MAX_VERSION
}
