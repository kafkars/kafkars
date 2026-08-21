//! Inert SCRAM credential-alteration intent with one submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, alter_user_scram_credentials::AlterUserScramCredentialsAdminRequest,
};

use super::AlterUserScramCredentials;

/// Inert caller-ordered SCRAM credential alteration request.
#[must_use = "call submit to admit the AlterUserScramCredentials operation"]
pub struct AlterUserScramCredentialsBuilder {
    engine: AdminEngine,
    request: AlterUserScramCredentialsAdminRequest,
    timeout: Duration,
}

impl AlterUserScramCredentialsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: AlterUserScramCredentialsAdminRequest,
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
    /// This is the sole public operation boundary. The engine captures its
    /// absolute deadline before translating, validating, or admitting secrets.
    pub fn submit(self) -> AlterUserScramCredentials {
        AlterUserScramCredentials::from_bridge(
            self.engine
                .submit_alter_user_scram_credentials(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for AlterUserScramCredentialsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterUserScramCredentialsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
