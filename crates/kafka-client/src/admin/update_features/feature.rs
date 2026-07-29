//! Stable explicit intent for one finalized-feature change.

/// Explicit direction and data-loss policy for one finalized-feature update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeatureUpdateIntent {
    /// Raises a finalized feature to one positive maximum version level.
    Upgrade,
    /// Lowers or deletes a feature only when Kafka deems the change lossless.
    SafeDowngrade,
    /// Lowers or deletes a feature even when Kafka deems the change lossy.
    UnsafeDowngrade,
}

/// One inert finalized-feature target in caller order.
///
/// There is deliberately no generic or default constructor: callers must name
/// upgrade, safe downgrade, or unsafe downgrade intent explicitly. Name,
/// level, batch-bound, and uniqueness validation occurs after `submit()` has
/// captured the public operation deadline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureUpdate {
    feature_name: String,
    max_version_level: i16,
    intent: FeatureUpdateIntent,
}

impl FeatureUpdate {
    /// Requests an upgrade to one positive finalized maximum version level.
    pub fn upgrade(feature_name: impl Into<String>, max_version_level: i16) -> Self {
        Self::with_intent(
            feature_name,
            max_version_level,
            FeatureUpdateIntent::Upgrade,
        )
    }

    /// Requests a lossless downgrade, or deletion when the level is zero.
    pub fn safe_downgrade(feature_name: impl Into<String>, max_version_level: i16) -> Self {
        Self::with_intent(
            feature_name,
            max_version_level,
            FeatureUpdateIntent::SafeDowngrade,
        )
    }

    /// Explicitly permits a lossy downgrade, or deletion when the level is zero.
    pub fn unsafe_downgrade(feature_name: impl Into<String>, max_version_level: i16) -> Self {
        Self::with_intent(
            feature_name,
            max_version_level,
            FeatureUpdateIntent::UnsafeDowngrade,
        )
    }

    /// Returns the exact finalized-feature name.
    pub fn feature_name(&self) -> &str {
        &self.feature_name
    }

    /// Returns the requested finalized maximum version level.
    pub const fn max_version_level(&self) -> i16 {
        self.max_version_level
    }

    /// Returns the caller's explicit direction and data-loss policy.
    pub const fn intent(&self) -> FeatureUpdateIntent {
        self.intent
    }

    /// Reports whether this downgrade requests finalized-feature deletion.
    pub const fn is_deletion(&self) -> bool {
        self.max_version_level == 0
    }

    pub(crate) fn into_parts(self) -> (String, i16, FeatureUpdateIntent) {
        (self.feature_name, self.max_version_level, self.intent)
    }

    fn with_intent(
        feature_name: impl Into<String>,
        max_version_level: i16,
        intent: FeatureUpdateIntent,
    ) -> Self {
        Self {
            feature_name: feature_name.into(),
            max_version_level,
            intent,
        }
    }
}
