//! Inert caller-ordered ACL creation intent with one submission boundary.

use std::time::{Duration, Instant};

use crate::bridge::{
    admin::AdminEngine,
    admin_create_acls::{AdminCreateAcls, CreateAclsAdminRequest},
};

use super::CreateAcls;

/// Inert caller-ordered batch of concrete ACL bindings.
#[must_use = "call submit to admit the CreateAcls operation"]
pub struct CreateAclsBuilder {
    engine: AdminEngine,
    request: CreateAclsAdminRequest,
    timeout: SubmissionTimeout,
}

impl CreateAclsBuilder {
    pub(crate) fn new(
        engine: AdminEngine,
        request: CreateAclsAdminRequest,
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
    /// captured before public result preparation or engine admission.
    pub fn submit(self) -> CreateAcls {
        let deadline = match self.timeout.capture() {
            Ok(deadline) => deadline,
            Err(CallDeadlineError::Elapsed) => {
                return CreateAcls::from_bridge(AdminCreateAcls::deadline_elapsed());
            }
            Err(CallDeadlineError::Overflow) => {
                return CreateAcls::from_bridge(AdminCreateAcls::invalid_deadline());
            }
        };
        CreateAcls::from_bridge(self.engine.submit_create_acls(self.request, deadline))
    }
}

impl std::fmt::Debug for CreateAclsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CreateAclsBuilder")
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

    pub(super) const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
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
