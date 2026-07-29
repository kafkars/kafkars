//! Canonical bounded feature metadata returned by one API-18 query.

use core::mem::size_of;

use super::{
    DescribeFeaturesFinalizedFeature, DescribeFeaturesSupportedFeature, DescribeFeaturesValueError,
};

/// Maximum features retained in either the supported or finalized collection.
pub const DESCRIBE_FEATURES_MAX_FEATURES_PER_COLLECTION: usize = 1024;
/// Maximum UTF-8 bytes retained for one feature name.
pub const DESCRIBE_FEATURES_MAX_FEATURE_NAME_BYTES: usize = 256;
/// Maximum aggregate logical UTF-8 feature-name bytes in both collections.
pub const DESCRIBE_FEATURES_MAX_FEATURE_TEXT_BYTES: usize = 256 * 1024;
/// Maximum concrete vector and string capacity retained through observation.
pub const DESCRIBE_FEATURES_MAX_RETAINED_BYTES: usize = 1024 * 1024;

/// Successful feature metadata in strict feature-name byte order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeFeaturesDescription {
    throttle_time_ms: u32,
    supported_features: Vec<DescribeFeaturesSupportedFeature>,
    supported_features_complete: bool,
    finalized_features_epoch: Option<i64>,
    finalized_features: Vec<DescribeFeaturesFinalizedFeature>,
    zk_migration_ready: bool,
}

impl DescribeFeaturesDescription {
    /// Validates, bounds, and canonicalizes one protocol-normalized response.
    pub fn new(
        throttle_time_ms: u32,
        mut supported_features: Vec<DescribeFeaturesSupportedFeature>,
        supported_features_complete: bool,
        finalized_features_epoch: Option<i64>,
        mut finalized_features: Vec<DescribeFeaturesFinalizedFeature>,
        zk_migration_ready: bool,
    ) -> Result<Self, DescribeFeaturesValueError> {
        validate_counts(&supported_features, &finalized_features)?;
        validate_epoch(finalized_features_epoch, &finalized_features)?;
        validate_supported(&supported_features)?;
        validate_finalized(&finalized_features)?;
        validate_text_bytes(&supported_features, &finalized_features)?;
        validate_retained_bytes(
            supported_features.capacity(),
            &supported_features,
            finalized_features.capacity(),
            &finalized_features,
        )?;
        supported_features
            .sort_unstable_by(|left, right| left.name().as_bytes().cmp(right.name().as_bytes()));
        finalized_features
            .sort_unstable_by(|left, right| left.name().as_bytes().cmp(right.name().as_bytes()));
        validate_unique_supported(&supported_features)?;
        validate_unique_finalized(&finalized_features)?;
        Ok(Self {
            throttle_time_ms,
            supported_features,
            supported_features_complete,
            finalized_features_epoch,
            finalized_features,
            zk_migration_ready,
        })
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns broker-supported features in strict UTF-8 byte order.
    pub fn supported_features(&self) -> &[DescribeFeaturesSupportedFeature] {
        &self.supported_features
    }

    /// Reports whether the selected API version includes min-level-zero features.
    pub const fn supported_features_complete(&self) -> bool {
        self.supported_features_complete
    }

    /// Returns the nonnegative finalized-features epoch, when known.
    pub const fn finalized_features_epoch(&self) -> Option<i64> {
        self.finalized_features_epoch
    }

    /// Returns finalized features in strict UTF-8 byte order.
    pub fn finalized_features(&self) -> &[DescribeFeaturesFinalizedFeature] {
        &self.finalized_features
    }

    /// Reports the controller's exact ZooKeeper migration-readiness fact.
    pub const fn zk_migration_ready(&self) -> bool {
        self.zk_migration_ready
    }

    /// Consumes this description into adapter-owned stable values.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        u32,
        Vec<DescribeFeaturesSupportedFeature>,
        bool,
        Option<i64>,
        Vec<DescribeFeaturesFinalizedFeature>,
        bool,
    ) {
        (
            self.throttle_time_ms,
            self.supported_features,
            self.supported_features_complete,
            self.finalized_features_epoch,
            self.finalized_features,
            self.zk_migration_ready,
        )
    }
}

fn validate_counts(
    supported: &[DescribeFeaturesSupportedFeature],
    finalized: &[DescribeFeaturesFinalizedFeature],
) -> Result<(), DescribeFeaturesValueError> {
    if supported.len() > DESCRIBE_FEATURES_MAX_FEATURES_PER_COLLECTION {
        return Err(DescribeFeaturesValueError::TooManySupportedFeatures);
    }
    if finalized.len() > DESCRIBE_FEATURES_MAX_FEATURES_PER_COLLECTION {
        return Err(DescribeFeaturesValueError::TooManyFinalizedFeatures);
    }
    Ok(())
}

fn validate_epoch(
    epoch: Option<i64>,
    finalized: &[DescribeFeaturesFinalizedFeature],
) -> Result<(), DescribeFeaturesValueError> {
    if epoch.is_some_and(|value| value < 0) {
        return Err(DescribeFeaturesValueError::NegativeFinalizedFeaturesEpoch);
    }
    if epoch.is_none() && !finalized.is_empty() {
        return Err(DescribeFeaturesValueError::FinalizedFeaturesWithoutEpoch);
    }
    Ok(())
}

fn validate_supported(
    features: &[DescribeFeaturesSupportedFeature],
) -> Result<(), DescribeFeaturesValueError> {
    for feature in features {
        if feature.name().is_empty() {
            return Err(DescribeFeaturesValueError::EmptySupportedFeatureName);
        }
        if feature.name().len() > DESCRIBE_FEATURES_MAX_FEATURE_NAME_BYTES {
            return Err(DescribeFeaturesValueError::SupportedFeatureNameTooLong);
        }
        if !feature.range_is_well_formed() {
            return Err(DescribeFeaturesValueError::InvalidSupportedFeatureRange);
        }
    }
    Ok(())
}

fn validate_finalized(
    features: &[DescribeFeaturesFinalizedFeature],
) -> Result<(), DescribeFeaturesValueError> {
    for feature in features {
        if feature.name().is_empty() {
            return Err(DescribeFeaturesValueError::EmptyFinalizedFeatureName);
        }
        if feature.name().len() > DESCRIBE_FEATURES_MAX_FEATURE_NAME_BYTES {
            return Err(DescribeFeaturesValueError::FinalizedFeatureNameTooLong);
        }
        if !feature.range_is_well_formed() {
            return Err(DescribeFeaturesValueError::InvalidFinalizedFeatureRange);
        }
    }
    Ok(())
}

fn validate_text_bytes(
    supported: &[DescribeFeaturesSupportedFeature],
    finalized: &[DescribeFeaturesFinalizedFeature],
) -> Result<(), DescribeFeaturesValueError> {
    let total = supported
        .iter()
        .map(|feature| feature.name().len())
        .chain(finalized.iter().map(|feature| feature.name().len()))
        .try_fold(0usize, usize::checked_add)
        .ok_or(DescribeFeaturesValueError::FeatureTextBytesExceeded)?;
    if total > DESCRIBE_FEATURES_MAX_FEATURE_TEXT_BYTES {
        return Err(DescribeFeaturesValueError::FeatureTextBytesExceeded);
    }
    Ok(())
}

fn validate_retained_bytes(
    supported_capacity: usize,
    supported: &[DescribeFeaturesSupportedFeature],
    finalized_capacity: usize,
    finalized: &[DescribeFeaturesFinalizedFeature],
) -> Result<(), DescribeFeaturesValueError> {
    let retained = size_of::<DescribeFeaturesDescription>()
        .checked_add(
            supported_capacity
                .checked_mul(size_of::<DescribeFeaturesSupportedFeature>())
                .ok_or(DescribeFeaturesValueError::RetainedBytesExceeded)?,
        )
        .and_then(|bytes| {
            finalized_capacity
                .checked_mul(size_of::<DescribeFeaturesFinalizedFeature>())
                .and_then(|entries| bytes.checked_add(entries))
        })
        .and_then(|bytes| {
            supported
                .iter()
                .map(DescribeFeaturesSupportedFeature::name_capacity)
                .chain(
                    finalized
                        .iter()
                        .map(DescribeFeaturesFinalizedFeature::name_capacity),
                )
                .try_fold(bytes, usize::checked_add)
        })
        .ok_or(DescribeFeaturesValueError::RetainedBytesExceeded)?;
    if retained > DESCRIBE_FEATURES_MAX_RETAINED_BYTES {
        return Err(DescribeFeaturesValueError::RetainedBytesExceeded);
    }
    Ok(())
}

fn validate_unique_supported(
    features: &[DescribeFeaturesSupportedFeature],
) -> Result<(), DescribeFeaturesValueError> {
    if features
        .windows(2)
        .any(|pair| pair[0].name() == pair[1].name())
    {
        return Err(DescribeFeaturesValueError::DuplicateSupportedFeature);
    }
    Ok(())
}

fn validate_unique_finalized(
    features: &[DescribeFeaturesFinalizedFeature],
) -> Result<(), DescribeFeaturesValueError> {
    if features
        .windows(2)
        .any(|pair| pair[0].name() == pair[1].name())
    {
        return Err(DescribeFeaturesValueError::DuplicateFinalizedFeature);
    }
    Ok(())
}
