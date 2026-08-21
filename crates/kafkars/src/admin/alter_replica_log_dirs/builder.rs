//! Inert caller-ordered replica log-directory alteration intent.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_alter_replica_log_dirs::AlterReplicaLogDirsAdminRequest,
};

use super::AlterReplicaLogDirs;

/// Inert caller-ordered replica-to-directory assignments.
#[must_use = "call submit to admit the AlterReplicaLogDirs operation"]
pub struct AlterReplicaLogDirsBuilder {
    engine: AdminEngine,
    request: AlterReplicaLogDirsAdminRequest,
    timeout: Duration,
}

impl AlterReplicaLogDirsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: AlterReplicaLogDirsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> AlterReplicaLogDirs {
        AlterReplicaLogDirs::from_bridge(
            self.engine
                .submit_alter_replica_log_dirs(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for AlterReplicaLogDirsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterReplicaLogDirsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
