//! Rejections for malformed or unbounded normalized feature metadata.

use core::fmt;

/// Invalid protocol-normalized `DescribeFeatures` value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeFeaturesValueError {
    /// The broker returned too many supported features.
    TooManySupportedFeatures,
    /// The broker returned too many finalized features.
    TooManyFinalizedFeatures,
    /// A supported feature name is empty.
    EmptySupportedFeatureName,
    /// A finalized feature name is empty.
    EmptyFinalizedFeatureName,
    /// A supported feature name exceeds the fixed UTF-8 byte limit.
    SupportedFeatureNameTooLong,
    /// A finalized feature name exceeds the fixed UTF-8 byte limit.
    FinalizedFeatureNameTooLong,
    /// Aggregate feature-name text exceeds the fixed response limit.
    FeatureTextBytesExceeded,
    /// Retained vectors or strings exceed the fixed terminal envelope.
    RetainedBytesExceeded,
    /// A supported feature range is negative or inverted.
    InvalidSupportedFeatureRange,
    /// A finalized feature range is negative or inverted.
    InvalidFinalizedFeatureRange,
    /// The supported feature collection repeats one name.
    DuplicateSupportedFeature,
    /// The finalized feature collection repeats one name.
    DuplicateFinalizedFeature,
    /// A present finalized-features epoch is negative.
    NegativeFinalizedFeaturesEpoch,
    /// Finalized feature entries were reported without a known epoch.
    FinalizedFeaturesWithoutEpoch,
}

impl fmt::Display for DescribeFeaturesValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid DescribeFeatures value: {self:?}")
    }
}

impl std::error::Error for DescribeFeaturesValueError {}
