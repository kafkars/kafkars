//! Generated-free bounded facts from one explicit feature description.

/// One broker-supported feature range, canonicalized by UTF-8 name bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeFeaturesSupportedFeature {
    name: String,
    min_version: i16,
    max_version: i16,
}

impl NormalizedDescribeFeaturesSupportedFeature {
    pub(super) const fn new(name: String, min_version: i16, max_version: i16) -> Self {
        Self {
            name,
            min_version,
            max_version,
        }
    }

    pub(crate) fn into_parts(self) -> (String, i16, i16) {
        (self.name, self.min_version, self.max_version)
    }

    pub(super) fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn name_capacity(&self) -> usize {
        self.name.capacity()
    }
}

/// One cluster-finalized feature range, canonicalized by UTF-8 name bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeFeaturesFinalizedFeature {
    name: String,
    min_version_level: i16,
    max_version_level: i16,
}

impl NormalizedDescribeFeaturesFinalizedFeature {
    pub(super) const fn new(name: String, min_version_level: i16, max_version_level: i16) -> Self {
        Self {
            name,
            min_version_level,
            max_version_level,
        }
    }

    pub(crate) fn into_parts(self) -> (String, i16, i16) {
        (self.name, self.min_version_level, self.max_version_level)
    }

    pub(super) fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn name_capacity(&self) -> usize {
        self.name.capacity()
    }
}

/// One bounded API-key 18 terminal with exact top-level broker status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeFeaturesResponse {
    throttle_time_ms: u32,
    broker_error_code: i16,
    supported_features: Vec<NormalizedDescribeFeaturesSupportedFeature>,
    supported_features_complete: bool,
    finalized_features_epoch: Option<i64>,
    finalized_features: Vec<NormalizedDescribeFeaturesFinalizedFeature>,
    zk_migration_ready: bool,
    retained_bytes: usize,
}

impl NormalizedDescribeFeaturesResponse {
    pub(super) const fn new(
        throttle_time_ms: u32,
        broker_error_code: i16,
        supported_features: Vec<NormalizedDescribeFeaturesSupportedFeature>,
        supported_features_complete: bool,
        finalized_features_epoch: Option<i64>,
        finalized_features: Vec<NormalizedDescribeFeaturesFinalizedFeature>,
        zk_migration_ready: bool,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            broker_error_code,
            supported_features,
            supported_features_complete,
            finalized_features_epoch,
            finalized_features,
            zk_migration_ready,
            retained_bytes,
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn into_parts(
        self,
    ) -> (
        u32,
        i16,
        Vec<NormalizedDescribeFeaturesSupportedFeature>,
        bool,
        Option<i64>,
        Vec<NormalizedDescribeFeaturesFinalizedFeature>,
        bool,
        usize,
    ) {
        (
            self.throttle_time_ms,
            self.broker_error_code,
            self.supported_features,
            self.supported_features_complete,
            self.finalized_features_epoch,
            self.finalized_features,
            self.zk_migration_ready,
            self.retained_bytes,
        )
    }
}
