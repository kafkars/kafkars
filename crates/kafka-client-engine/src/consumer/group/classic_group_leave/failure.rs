//! Closed translation from driver request failures into close terminal facts.

use crate::driver::ClassicGroupLeaveDriverFailureKind;

use super::completion::GroupConsumerCloseTerminalFailureKind as Failure;

pub(super) const fn classify_leave_request_error(
    error: ClassicGroupLeaveDriverFailureKind,
) -> Failure {
    match error {
        ClassicGroupLeaveDriverFailureKind::DeadlineElapsed => Failure::DeadlineElapsed,
        ClassicGroupLeaveDriverFailureKind::Compatibility => Failure::Compatibility,
        ClassicGroupLeaveDriverFailureKind::DriverRejected => Failure::DriverRejected,
        ClassicGroupLeaveDriverFailureKind::Transport => Failure::Transport,
        ClassicGroupLeaveDriverFailureKind::InvalidResponse => Failure::InvalidResponse,
        ClassicGroupLeaveDriverFailureKind::ResponseTooLarge => Failure::ResponseTooLarge,
        ClassicGroupLeaveDriverFailureKind::Authentication => Failure::Authentication,
    }
}
