//! Inert leader election intent with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, admin_elect_leaders::ElectLeadersAdminRequest};

use super::ElectLeaders;

/// Inert selected-partition or cluster-wide leader election.
#[must_use = "call submit to admit the ElectLeaders operation"]
pub struct ElectLeadersBuilder {
    engine: AdminEngine,
    request: ElectLeadersAdminRequest,
    timeout: Duration,
}

impl ElectLeadersBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: ElectLeadersAdminRequest,
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

    /// Captures the public deadline, attempts bounded admission, and returns
    /// one named observer.
    pub fn submit(self) -> ElectLeaders {
        ElectLeaders::from_bridge(self.engine.submit_elect_leaders(self.request, self.timeout))
    }
}

impl std::fmt::Debug for ElectLeadersBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ElectLeadersBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
