//! Public entry point for one inert delegation-token description.

use super::Admin;
use crate::admin::DescribeDelegationTokensBuilder;

impl Admin {
    /// Builds an inert all-visible delegation-token query.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DescribeDelegationTokensBuilder::submit`] is called. Use
    /// [`DescribeDelegationTokensBuilder::owners`] for an explicit nonempty
    /// owner selection.
    pub fn describe_delegation_tokens(&self) -> DescribeDelegationTokensBuilder {
        DescribeDelegationTokensBuilder::new(self.engine.clone(), self.engine.default_timeout())
    }
}
