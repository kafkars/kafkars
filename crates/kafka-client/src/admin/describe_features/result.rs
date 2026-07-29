//! Stable successful Kafka supported and finalized feature metadata.

use std::time::Duration;

use super::{FinalizedFeature, SupportedFeature};

/// One bounded canonical Kafka feature metadata snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeFeaturesResult {
    throttle_time: Duration,
    supported_features: Vec<SupportedFeature>,
    supported_features_complete: bool,
    finalized_features_epoch: Option<i64>,
    finalized_features: Vec<FinalizedFeature>,
    zk_migration_ready: bool,
}

impl DescribeFeaturesResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        supported_features: Vec<SupportedFeature>,
        supported_features_complete: bool,
        finalized_features_epoch: Option<i64>,
        finalized_features: Vec<FinalizedFeature>,
        zk_migration_ready: bool,
    ) -> Self {
        Self {
            throttle_time,
            supported_features,
            supported_features_complete,
            finalized_features_epoch,
            finalized_features,
            zk_migration_ready,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns unique supported features in strict UTF-8 byte order.
    pub fn supported_features(&self) -> &[SupportedFeature] {
        &self.supported_features
    }

    /// Reports whether the selected API version includes min-level-zero features.
    pub const fn supported_features_complete(&self) -> bool {
        self.supported_features_complete
    }

    /// Returns the nonnegative cluster finalized-feature epoch when known.
    pub const fn finalized_features_epoch(&self) -> Option<i64> {
        self.finalized_features_epoch
    }

    /// Returns unique finalized features in strict UTF-8 byte order.
    pub fn finalized_features(&self) -> &[FinalizedFeature] {
        &self.finalized_features
    }

    /// Reports Kafka's exact ZK-migration readiness fact.
    pub const fn zk_migration_ready(&self) -> bool {
        self.zk_migration_ready
    }

    /// Consumes the result into its stable generated-free parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        Duration,
        Vec<SupportedFeature>,
        bool,
        Option<i64>,
        Vec<FinalizedFeature>,
        bool,
    ) {
        (
            self.throttle_time,
            self.supported_features,
            self.supported_features_complete,
            self.finalized_features_epoch,
            self.finalized_features,
            self.zk_migration_ready,
        )
    }
}
