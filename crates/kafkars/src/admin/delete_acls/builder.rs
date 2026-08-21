//! Inert caller-ordered ACL deletion intent with one submission boundary.

use std::time::{Duration, Instant};

use crate::bridge::{
    admin::AdminEngine,
    admin_delete_acls::{AdminDeleteAcls, DeleteAclsAdminRequest},
};

use super::DeleteAcls;

/// Inert caller-ordered batch of ACL deletion filters.
#[must_use = "call submit to admit the DeleteAcls operation"]
pub struct DeleteAclsBuilder {
    engine: AdminEngine,
    request: DeleteAclsAdminRequest,
    timeout: SubmissionTimeout,
}

impl DeleteAclsBuilder {
    pub(crate) fn new(
        engine: AdminEngine,
        request: DeleteAclsAdminRequest,
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
    pub fn submit(self) -> DeleteAcls {
        let deadline = match self.timeout.capture() {
            Ok(deadline) => deadline,
            Err(CallDeadlineError::Elapsed) => {
                return DeleteAcls::from_bridge(AdminDeleteAcls::deadline_elapsed());
            }
            Err(CallDeadlineError::Overflow) => {
                return DeleteAcls::from_bridge(AdminDeleteAcls::invalid_deadline());
            }
        };
        DeleteAcls::from_bridge(self.engine.submit_delete_acls(self.request, deadline))
    }
}

impl std::fmt::Debug for DeleteAclsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteAclsBuilder")
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
