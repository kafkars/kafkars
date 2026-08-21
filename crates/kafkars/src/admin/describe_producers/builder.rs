//! Inert Admin `DescribeProducers` intent with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, describe_producers::DescribeProducersAdminRequest};

use super::DescribeProducers;

/// Inert caller-ordered Admin `DescribeProducers` request.
#[must_use = "call submit to admit the DescribeProducers operation"]
pub struct DescribeProducersBuilder {
    engine: AdminEngine,
    request: DescribeProducersAdminRequest,
    timeout: Duration,
}

impl DescribeProducersBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DescribeProducersAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Routes every partition query to one exact nonnegative broker.
    ///
    /// Validation remains deferred until [`Self::submit`]. Without this
    /// option, each partition query retains its existing leader route.
    pub fn broker_id(mut self, broker_id: i32) -> Self {
        self.request = self.request.with_broker_id(broker_id);
        self
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
    pub fn submit(self) -> DescribeProducers {
        DescribeProducers::from_bridge(
            self.engine
                .submit_describe_producers(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeProducersBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeProducersBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
