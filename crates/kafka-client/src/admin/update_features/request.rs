//! Bounded caller-ordered request retained until the submission boundary.

use core::fmt;
use std::collections::BTreeSet;

use super::{FeatureUpdate, FeatureUpdateIntent};

pub(super) const MAX_UPDATES: usize = 1024;
pub(super) const MAX_FEATURE_NAME_BYTES: usize = 256;
pub(super) const MAX_FEATURE_TEXT_BYTES: usize = 64 * 1024;

/// Inert request translated by the private bridge after deadline capture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UpdateFeaturesRequest {
    updates: Vec<FeatureUpdate>,
    validate_only: bool,
}

impl UpdateFeaturesRequest {
    pub(crate) const fn new(updates: Vec<FeatureUpdate>) -> Self {
        Self {
            updates,
            validate_only: false,
        }
    }

    pub(crate) fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), UpdateFeaturesRequestError> {
        if self.updates.is_empty() {
            return Err(UpdateFeaturesRequestError::EmptyBatch);
        }
        if self.updates.len() > MAX_UPDATES {
            return Err(UpdateFeaturesRequestError::TooManyUpdates);
        }

        let mut names = BTreeSet::new();
        let mut total_name_bytes = 0usize;
        for update in &self.updates {
            validate_update(update)?;
            total_name_bytes = total_name_bytes
                .checked_add(update.feature_name().len())
                .ok_or(UpdateFeaturesRequestError::FeatureTextBytesExceeded)?;
            if total_name_bytes > MAX_FEATURE_TEXT_BYTES {
                return Err(UpdateFeaturesRequestError::FeatureTextBytesExceeded);
            }
            if !names.insert(update.feature_name()) {
                return Err(UpdateFeaturesRequestError::DuplicateFeature);
            }
        }
        Ok(())
    }

    pub(crate) fn into_parts(self) -> (Vec<FeatureUpdate>, bool) {
        (self.updates, self.validate_only)
    }
}

fn validate_update(update: &FeatureUpdate) -> Result<(), UpdateFeaturesRequestError> {
    if update.feature_name().is_empty() {
        return Err(UpdateFeaturesRequestError::EmptyFeatureName);
    }
    if update.feature_name().len() > MAX_FEATURE_NAME_BYTES {
        return Err(UpdateFeaturesRequestError::FeatureNameTooLong);
    }
    if update.max_version_level() < 0 {
        return Err(UpdateFeaturesRequestError::NegativeVersionLevel);
    }
    if update.max_version_level() == 0 && update.intent() == FeatureUpdateIntent::Upgrade {
        return Err(UpdateFeaturesRequestError::DeletionRequiresDowngrade);
    }
    Ok(())
}

/// Invalid finalized-feature update intent detected after deadline capture.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UpdateFeaturesRequestError {
    EmptyBatch,
    TooManyUpdates,
    EmptyFeatureName,
    FeatureNameTooLong,
    FeatureTextBytesExceeded,
    NegativeVersionLevel,
    DeletionRequiresDowngrade,
    DuplicateFeature,
}

impl fmt::Display for UpdateFeaturesRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid UpdateFeatures request: {self:?}")
    }
}

impl std::error::Error for UpdateFeaturesRequestError {}
