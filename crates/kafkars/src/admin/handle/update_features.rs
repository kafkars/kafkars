//! Finalized-feature mutation entry point on the shared admin handle.

use super::Admin;
use crate::admin::{FeatureUpdate, UpdateFeaturesBuilder, UpdateFeaturesRequest};

impl Admin {
    /// Builds inert caller-ordered finalized-feature update intent.
    ///
    /// Callers must explicitly select upgrade, safe downgrade, or unsafe
    /// downgrade for every feature. No timeout starts and no operation is
    /// admitted until [`UpdateFeaturesBuilder::submit`] is called.
    pub fn update_features(&self, updates: Vec<FeatureUpdate>) -> UpdateFeaturesBuilder {
        UpdateFeaturesBuilder::new(
            self.engine.clone(),
            UpdateFeaturesRequest::new(updates),
            self.engine.default_timeout(),
        )
    }
}
