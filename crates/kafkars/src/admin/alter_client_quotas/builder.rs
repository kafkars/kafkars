//! Inert client-quota alteration intent with one submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, alter_client_quotas::AlterClientQuotasAdminRequest};

use super::AlterClientQuotas;

/// Inert caller-ordered client-quota alteration request.
#[must_use = "call submit to admit the AlterClientQuotas operation"]
pub struct AlterClientQuotasBuilder {
    engine: AdminEngine,
    request: AlterClientQuotasAdminRequest,
    timeout: Duration,
}

impl AlterClientQuotasBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: AlterClientQuotasAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Selects broker-side validation without changing client quotas.
    ///
    /// The default is `false`.
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.request = self.request.with_validate_only(validate_only);
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
    pub fn submit(self) -> AlterClientQuotas {
        AlterClientQuotas::from_bridge(
            self.engine
                .submit_alter_client_quotas(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for AlterClientQuotasBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterClientQuotasBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
