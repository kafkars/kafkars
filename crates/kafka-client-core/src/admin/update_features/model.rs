//! Bounded caller-ordered intent for one finalized-feature update request.

use core::fmt;
use std::collections::BTreeSet;

/// Maximum finalized features admitted in one request.
pub const UPDATE_FEATURES_MAX_UPDATES: usize = 1024;
/// Maximum UTF-8 bytes retained for one finalized-feature name.
pub const UPDATE_FEATURES_MAX_FEATURE_NAME_BYTES: usize = 256;
/// Maximum aggregate UTF-8 feature-name bytes retained by one request.
pub const UPDATE_FEATURES_MAX_FEATURE_TEXT_BYTES: usize = 64 * 1024;

/// Explicit direction and loss policy for one finalized-feature update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateFeatureIntent {
    /// Raise a finalized feature to the requested positive level.
    Upgrade,
    /// Lower or delete a finalized feature only when Kafka deems it lossless.
    SafeDowngrade,
    /// Lower or delete a finalized feature even when Kafka deems it lossy.
    UnsafeDowngrade,
}

/// One finalized-feature target retained in exact caller order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeature {
    feature: String,
    max_version_level: i16,
    intent: UpdateFeatureIntent,
}

impl UpdateFeature {
    /// Creates inert update data for validation by the enclosing plan.
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

    /// Returns the requested nonnegative finalized maximum level.
    pub const fn max_version_level(&self) -> i16 {
        self.max_version_level
    }

    /// Returns the explicit direction and loss policy.
    pub const fn intent(&self) -> UpdateFeatureIntent {
        self.intent
    }

    /// Reports whether level zero requests finalized-feature deletion.
    pub const fn is_deletion(&self) -> bool {
        self.max_version_level == 0
    }

    /// Consumes this update into adapter-owned scalar parts.
    pub fn into_parts(self) -> (String, i16, UpdateFeatureIntent) {
        (self.feature, self.max_version_level, self.intent)
    }
}

/// Validated intent for one destructive controller `UpdateFeatures` RPC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeaturesPlan {
    updates: Vec<UpdateFeature>,
    validate_only: bool,
}

impl UpdateFeaturesPlan {
    /// Validates bounds and unique identities while preserving caller order.
    pub fn new(
        updates: Vec<UpdateFeature>,
        validate_only: bool,
    ) -> Result<Self, UpdateFeaturesPlanError> {
        if updates.is_empty() {
            return Err(UpdateFeaturesPlanError::EmptyBatch);
        }
        if updates.len() > UPDATE_FEATURES_MAX_UPDATES {
            return Err(UpdateFeaturesPlanError::TooManyUpdates);
        }
        let mut names = BTreeSet::new();
        let mut total_name_bytes = 0usize;
        for update in &updates {
            validate_update(update)?;
            total_name_bytes = total_name_bytes
                .checked_add(update.feature.len())
                .ok_or(UpdateFeaturesPlanError::FeatureTextBytesExceeded)?;
            if total_name_bytes > UPDATE_FEATURES_MAX_FEATURE_TEXT_BYTES {
                return Err(UpdateFeaturesPlanError::FeatureTextBytesExceeded);
            }
            if !names.insert(update.feature.as_str()) {
                return Err(UpdateFeaturesPlanError::DuplicateFeature);
            }
        }
        Ok(Self {
            updates,
            validate_only,
        })
    }

    /// Returns finalized-feature updates in exact caller order.
    pub fn updates(&self) -> &[UpdateFeature] {
        &self.updates
    }

    /// Returns whether Kafka should validate without mutating feature state.
    pub const fn validate_only(&self) -> bool {
        self.validate_only
    }

    /// Consumes this plan into adapter-owned request parts.
    pub fn into_parts(self) -> (Vec<UpdateFeature>, bool) {
        (self.updates, self.validate_only)
    }
}

fn validate_update(update: &UpdateFeature) -> Result<(), UpdateFeaturesPlanError> {
    if update.feature.is_empty() {
        return Err(UpdateFeaturesPlanError::EmptyFeatureName);
    }
    if update.feature.len() > UPDATE_FEATURES_MAX_FEATURE_NAME_BYTES {
        return Err(UpdateFeaturesPlanError::FeatureNameTooLong);
    }
    if update.max_version_level < 0 {
        return Err(UpdateFeaturesPlanError::NegativeVersionLevel);
    }
    if update.max_version_level == 0 && update.intent == UpdateFeatureIntent::Upgrade {
        return Err(UpdateFeaturesPlanError::DeletionRequiresDowngrade);
    }
    Ok(())
}

/// Invalid deterministic finalized-feature update intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesPlanError {
    /// Kafka cannot execute an empty feature-update batch.
    EmptyBatch,
    /// One operation cannot retain more than 1024 updates.
    TooManyUpdates,
    /// Finalized-feature names must not be empty.
    EmptyFeatureName,
    /// A finalized-feature name cannot exceed 256 UTF-8 bytes.
    FeatureNameTooLong,
    /// Aggregate finalized-feature name text cannot exceed 64 KiB.
    FeatureTextBytesExceeded,
    /// Finalized-feature levels cannot be negative.
    NegativeVersionLevel,
    /// Level zero deletion requires safe or unsafe downgrade intent.
    DeletionRequiresDowngrade,
    /// One operation cannot update the same finalized feature twice.
    DuplicateFeature,
}

impl fmt::Display for UpdateFeaturesPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid UpdateFeatures plan: {self:?}")
    }
}

impl std::error::Error for UpdateFeaturesPlanError {}
