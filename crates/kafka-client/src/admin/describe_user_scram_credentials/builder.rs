//! Inert SCRAM credential-description intent with one submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, describe_user_scram_credentials::DescribeUserScramCredentialsAdminRequest,
};

use super::DescribeUserScramCredentials;

/// Inert query for users' SCRAM credential metadata.
///
/// With no explicit user filter, the query selects every user visible to the
/// authenticated principal.
#[must_use = "call submit to admit the DescribeUserScramCredentials operation"]
pub struct DescribeUserScramCredentialsBuilder {
    engine: AdminEngine,
    request: DescribeUserScramCredentialsAdminRequest,
    timeout: Duration,
}

impl DescribeUserScramCredentialsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DescribeUserScramCredentialsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Replaces the optional filter with a caller-ordered set of users.
    ///
    /// Construction remains inert. Empty and duplicate filters are rejected
    /// only when [`Self::submit`] reaches bounded engine admission.
    pub fn users<I, T>(mut self, users: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.request = self
            .request
            .with_users(users.into_iter().map(Into::into).collect());
        self
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attempts immediate bounded admission and returns one named observer.
    ///
    /// This is the sole public operation boundary. The engine captures its
    /// absolute deadline before validation or admission.
    pub fn submit(self) -> DescribeUserScramCredentials {
        DescribeUserScramCredentials::from_bridge(
            self.engine
                .submit_describe_user_scram_credentials(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeUserScramCredentialsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeUserScramCredentialsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
