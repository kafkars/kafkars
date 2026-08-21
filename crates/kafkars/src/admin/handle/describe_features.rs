//! Kafka feature-metadata entry point on the shared admin handle.

use super::Admin;
use crate::admin::DescribeFeaturesBuilder;

impl Admin {
    /// Builds inert intent to describe broker-supported and cluster-finalized features.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DescribeFeaturesBuilder::submit`] is called.
    pub fn describe_features(&self) -> DescribeFeaturesBuilder {
        DescribeFeaturesBuilder::new(self.engine.clone(), self.engine.default_timeout())
    }
}
