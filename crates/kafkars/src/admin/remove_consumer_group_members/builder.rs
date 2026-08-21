//! Inert static-member removal intent with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_remove_consumer_group_members::RemoveConsumerGroupMembersAdminRequest,
};

use super::RemoveConsumerGroupMembers;

/// Inert caller-ordered selected static-member removal.
#[must_use = "call submit to admit the RemoveConsumerGroupMembers operation"]
pub struct RemoveConsumerGroupMembersBuilder {
    engine: AdminEngine,
    request: RemoveConsumerGroupMembersAdminRequest,
    timeout: Duration,
}

impl RemoveConsumerGroupMembersBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: RemoveConsumerGroupMembersAdminRequest,
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

    /// Adds a broker-visible reason, requiring `LeaveGroup` v5 or newer.
    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.request.set_reason(reason.into());
        self
    }

    /// Attempts bounded admission and returns one named observer.
    pub fn submit(self) -> RemoveConsumerGroupMembers {
        RemoveConsumerGroupMembers::from_bridge(
            self.engine
                .submit_remove_consumer_group_members(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for RemoveConsumerGroupMembersBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoveConsumerGroupMembersBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
