//! Stable generated-free feature metadata returned by Admin `DescribeFeatures`.

/// One broker-supported feature range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeFeaturesSupportedFeature {
    pub(super) name: String,
    pub(super) min_version: i16,
    pub(super) max_version: i16,
}

impl DescribeFeaturesSupportedFeature {
    /// Returns the exact feature name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the broker's minimum supported level.
    pub const fn min_version(&self) -> i16 {
        self.min_version
    }

    /// Returns the broker's maximum supported level.
    pub const fn max_version(&self) -> i16 {
        self.max_version
    }

    /// Consumes this fact into stable scalar parts.
    pub fn into_parts(self) -> (String, i16, i16) {
        (self.name, self.min_version, self.max_version)
    }
}

/// One cluster-finalized feature range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeFeaturesFinalizedFeature {
    pub(super) name: String,
    pub(super) min_version_level: i16,
    pub(super) max_version_level: i16,
}

impl DescribeFeaturesFinalizedFeature {
    /// Returns the exact feature name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the cluster-wide minimum finalized level.
    pub const fn min_version_level(&self) -> i16 {
        self.min_version_level
    }

    /// Returns the cluster-wide maximum finalized level.
    pub const fn max_version_level(&self) -> i16 {
        self.max_version_level
    }

    /// Consumes this fact into stable scalar parts.
    pub fn into_parts(self) -> (String, i16, i16) {
        (self.name, self.min_version_level, self.max_version_level)
    }
}

/// Complete bounded feature metadata and Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeFeaturesDescription {
    pub(super) throttle_time_ms: u32,
    pub(super) supported_features: Vec<DescribeFeaturesSupportedFeature>,
    pub(super) supported_features_complete: bool,
    pub(super) finalized_features_epoch: Option<i64>,
    pub(super) finalized_features: Vec<DescribeFeaturesFinalizedFeature>,
    pub(super) zk_migration_ready: bool,
}

impl DescribeFeaturesDescription {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Reports whether Kafka returned every supported feature, including level-zero entries.
    pub const fn supported_features_complete(&self) -> bool {
        self.supported_features_complete
    }

    /// Returns broker-supported features in strict UTF-8 byte order.
    pub fn supported_features(&self) -> &[DescribeFeaturesSupportedFeature] {
        &self.supported_features
    }

    /// Returns the nonnegative finalized-features epoch, when known.
    pub const fn finalized_features_epoch(&self) -> Option<i64> {
        self.finalized_features_epoch
    }

    /// Returns finalized features in strict UTF-8 byte order.
    pub fn finalized_features(&self) -> &[DescribeFeaturesFinalizedFeature] {
        &self.finalized_features
    }

    /// Reports Kafka's exact ZooKeeper migration-readiness fact.
    pub const fn zk_migration_ready(&self) -> bool {
        self.zk_migration_ready
    }

    /// Consumes the description into stable scalar parts.
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
