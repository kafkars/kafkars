//! Inert `DescribeConfigs` options with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, admin_configs_request::DescribeConfigsAdminRequest};

use super::DescribeConfigs;

/// Inert ordered topic-configuration request.
#[must_use = "call submit to admit the DescribeConfigs operation"]
pub struct DescribeConfigsBuilder {
    engine: AdminEngine,
    request: DescribeConfigsAdminRequest,
    timeout: Duration,
}

impl DescribeConfigsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DescribeConfigsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Requests Kafka's configuration synonyms.
    pub fn include_synonyms(mut self, include: bool) -> Self {
        self.request = self.request.with_include_synonyms(include);
        self
    }

    /// Requests configuration documentation when the broker version supports it.
    pub fn include_documentation(mut self, include: bool) -> Self {
        self.request = self.request.with_include_documentation(include);
        self
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attempts immediate bounded admission and returns one named observer.
    pub fn submit(self) -> DescribeConfigs {
        DescribeConfigs::from_bridge(
            self.engine
                .submit_describe_configs(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeConfigsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeConfigsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
