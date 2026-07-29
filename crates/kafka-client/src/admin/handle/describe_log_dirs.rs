//! Log-directory description entry point on the shared public admin handle.

use super::Admin;
use crate::{
    admin::DescribeLogDirsBuilder, bridge::admin_describe_log_dirs::DescribeLogDirsAdminRequest,
};

impl Admin {
    /// Builds an inert caller-ordered log-directory query for selected brokers.
    ///
    /// By default every topic-partition on each broker is selected. No timeout
    /// starts and no operation is admitted until
    /// [`DescribeLogDirsBuilder::submit`] is called.
    pub fn describe_log_dirs<I>(&self, broker_ids: I) -> DescribeLogDirsBuilder
    where
        I: IntoIterator<Item = i32>,
    {
        let request = DescribeLogDirsAdminRequest::new(broker_ids.into_iter().collect());
        DescribeLogDirsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }
}
