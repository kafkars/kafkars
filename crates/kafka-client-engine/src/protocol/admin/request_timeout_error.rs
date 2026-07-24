//! Admin request-field validation layered over the shared deadline mechanism.

use kafka_client_core::{Deadline, Moment};

use super::super::request_timeout::{RequestDeadlineError, remaining_timeout_ms as shared_timeout};

/// Failure to construct one generated admin request timeout field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdminRequestDeadlineError {
    /// A caller supplied an impossible negative generated field.
    NegativeTimeout,
    /// The original absolute operation deadline already elapsed.
    DeadlineElapsed,
}

impl From<RequestDeadlineError> for AdminRequestDeadlineError {
    fn from(error: RequestDeadlineError) -> Self {
        match error {
            RequestDeadlineError::DeadlineElapsed => Self::DeadlineElapsed,
        }
    }
}

/// Adapts the shared deadline mechanism into the admin request error domain.
pub(crate) fn remaining_timeout_ms(
    now: Moment,
    deadline: Deadline,
) -> Result<i32, AdminRequestDeadlineError> {
    shared_timeout(now, deadline).map_err(AdminRequestDeadlineError::from)
}
