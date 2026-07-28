//! Inert client-quota description intent with one submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, describe_client_quotas::DescribeClientQuotasAdminRequest};

use super::DescribeClientQuotas;

/// Inert client-quota query.
///
/// An empty component set with the default non-strict match selects all quota
/// entities.
#[must_use = "call submit to admit the DescribeClientQuotas operation"]
pub struct DescribeClientQuotasBuilder {
    engine: AdminEngine,
    request: DescribeClientQuotasAdminRequest,
    timeout: Duration,
}

impl DescribeClientQuotasBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DescribeClientQuotasAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Sets whether entities with unspecified entity types are excluded.
    ///
    /// The default is `false`.
    pub fn strict(mut self, strict: bool) -> Self {
        self.request = self.request.with_strict(strict);
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
    pub fn submit(self) -> DescribeClientQuotas {
        DescribeClientQuotas::from_bridge(
            self.engine
                .submit_describe_client_quotas(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeClientQuotasBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeClientQuotasBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
