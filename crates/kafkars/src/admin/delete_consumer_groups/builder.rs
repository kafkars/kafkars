//! Inert Admin `DeleteConsumerGroups` intent with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_delete_consumer_groups::DeleteConsumerGroupsAdminRequest,
};

use super::DeleteConsumerGroups;

/// Inert caller-ordered Admin `DeleteConsumerGroups` request.
#[must_use = "call submit to admit the DeleteConsumerGroups operation"]
pub struct DeleteConsumerGroupsBuilder {
    engine: AdminEngine,
    request: DeleteConsumerGroupsAdminRequest,
    timeout: Duration,
}

impl DeleteConsumerGroupsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DeleteConsumerGroupsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attempts immediate bounded admission and returns one named observer.
    ///
    /// This is the public operation boundary. The engine captures its absolute
    /// deadline before validation or admission.
    pub fn submit(self) -> DeleteConsumerGroups {
        DeleteConsumerGroups::from_bridge(
            self.engine
                .submit_delete_consumer_groups(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DeleteConsumerGroupsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteConsumerGroupsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
