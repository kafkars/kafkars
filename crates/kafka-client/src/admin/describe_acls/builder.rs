//! Inert ACL description intent with one submission boundary.

use std::time::{Duration, Instant};

use crate::bridge::{
    admin::AdminEngine,
    admin_describe_acls::{AdminDescribeAcls, DescribeAclsAdminRequest},
};

use super::DescribeAcls;

/// Inert query for ACL bindings selected by one exact filter.
#[must_use = "call submit to admit the DescribeAcls operation"]
pub struct DescribeAclsBuilder {
    engine: AdminEngine,
    request: DescribeAclsAdminRequest,
    timeout: SubmissionTimeout,
}

impl DescribeAclsBuilder {
    pub(crate) fn new(
        engine: AdminEngine,
        request: DescribeAclsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout: SubmissionTimeout::new(timeout),
        }
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = self.timeout.with_timeout(timeout);
        self
    }

    /// Attempts immediate bounded admission and returns one named observer.
    ///
    /// This call is the public operation boundary. Its absolute deadline is
    /// captured before request translation or engine admission.
    pub fn submit(self) -> DescribeAcls {
        let deadline = match self.timeout.capture() {
            Ok(deadline) => deadline,
            Err(CallDeadlineError::Elapsed) => {
                return DescribeAcls::from_bridge(AdminDescribeAcls::deadline_elapsed());
            }
            Err(CallDeadlineError::Overflow) => {
                return DescribeAcls::from_bridge(AdminDescribeAcls::invalid_deadline());
            }
        };
        DescribeAcls::from_bridge(self.engine.submit_describe_acls(self.request, deadline))
    }
}

impl std::fmt::Debug for DescribeAclsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeAclsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout.timeout)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct SubmissionTimeout {
    timeout: Duration,
}

impl SubmissionTimeout {
    pub(super) const fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    pub(super) const fn with_timeout(self, timeout: Duration) -> Self {
        Self { timeout }
    }

    fn capture(self) -> Result<Instant, CallDeadlineError> {
        self.capture_at(Instant::now())
    }

    pub(super) fn capture_at(self, boundary: Instant) -> Result<Instant, CallDeadlineError> {
        let deadline = boundary
            .checked_add(self.timeout)
            .ok_or(CallDeadlineError::Overflow)?;
        if self.timeout.is_zero() {
            Err(CallDeadlineError::Elapsed)
        } else {
            Ok(deadline)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CallDeadlineError {
    Elapsed,
    Overflow,
}
