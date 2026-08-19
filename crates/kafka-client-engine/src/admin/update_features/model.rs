//! Engine-owned inert intent for Admin `UpdateFeatures`.

use kafka_client_core::{
    UpdateFeature as CoreFeature, UpdateFeatureIntent as CoreIntent,
    UpdateFeaturesPlan as CorePlan, UpdateFeaturesPlanError as CorePlanError,
};

pub(crate) enum UpdateFeaturesPlanFailure {
    Invalid,
    RetainedBytes,
}

/// Explicit direction and loss policy for one finalized-feature update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateFeatureIntent {
    /// Raises or establishes a finalized feature level.
    Upgrade,
    /// Permits only a broker-classified lossless downgrade or deletion.
    SafeDowngrade,
    /// Explicitly permits a potentially lossy downgrade or deletion.
    UnsafeDowngrade,
}

/// One inert finalized-feature target in caller order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeature {
    feature: String,
    max_version_level: i16,
    intent: UpdateFeatureIntent,
}

impl UpdateFeature {
    /// Creates inert update data validated only after deadline capture.
    pub const fn new(feature: String, max_version_level: i16, intent: UpdateFeatureIntent) -> Self {
        Self {
            feature,
            max_version_level,
            intent,
        }
    }

    /// Returns the exact finalized-feature name.
    pub fn feature(&self) -> &str {
        &self.feature
    }

    /// Returns the requested finalized maximum level.
    pub const fn max_version_level(&self) -> i16 {
        self.max_version_level
    }

    /// Returns the explicit direction and loss policy.
    pub const fn intent(&self) -> UpdateFeatureIntent {
        self.intent
    }

    fn to_core(&self) -> CoreFeature {
        CoreFeature::new(
            self.feature.clone().into_boxed_str().into_string(),
            self.max_version_level,
            match self.intent {
                UpdateFeatureIntent::Upgrade => CoreIntent::Upgrade,
                UpdateFeatureIntent::SafeDowngrade => CoreIntent::SafeDowngrade,
                UpdateFeatureIntent::UnsafeDowngrade => CoreIntent::UnsafeDowngrade,
            },
        )
    }
}

/// One inert caller-ordered finalized-feature update request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeaturesRequest {
    updates: Vec<UpdateFeature>,
    validate_only: bool,
}

impl UpdateFeaturesRequest {
    /// Creates inert intent. Validation remains deferred until submission.
    pub const fn new(updates: Vec<UpdateFeature>, validate_only: bool) -> Self {
        Self {
            updates,
            validate_only,
        }
    }

    /// Returns finalized-feature updates in caller order.
    pub fn updates(&self) -> &[UpdateFeature] {
        &self.updates
    }

    /// Returns whether Kafka should validate without mutating feature state.
    pub const fn validate_only(&self) -> bool {
        self.validate_only
    }

    pub(crate) fn plan(&self) -> Result<CorePlan, UpdateFeaturesPlanFailure> {
        let mut updates = Vec::new();
        updates
            .try_reserve_exact(self.updates.len())
            .map_err(|_| UpdateFeaturesPlanFailure::RetainedBytes)?;
        updates.extend(self.updates.iter().map(UpdateFeature::to_core));
        CorePlan::new(updates, self.validate_only)
            .map_err(|_error: CorePlanError| UpdateFeaturesPlanFailure::Invalid)
    }
}
