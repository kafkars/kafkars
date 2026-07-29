//! Configuration-resource listing entry point on the shared admin handle.

use super::Admin;
use crate::admin::ListConfigResourcesBuilder;

impl Admin {
    /// Builds inert intent to list configuration-resource identities.
    ///
    /// An empty resource-type filter requests every broker-supported type. No
    /// timeout starts and no operation is admitted until
    /// [`ListConfigResourcesBuilder::submit`] is called.
    pub fn list_config_resources(&self) -> ListConfigResourcesBuilder {
        ListConfigResourcesBuilder::new(self.engine.clone(), self.engine.default_timeout())
    }
}
