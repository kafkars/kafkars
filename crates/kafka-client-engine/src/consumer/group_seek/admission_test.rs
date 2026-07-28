//! Compile-time group-seek admission surface contract.

use super::{
    GroupConsumerSeekAdmissionError, GroupConsumerSeekAdmissionErrorKind, GroupConsumerSeekCapture,
};
use crate::consumer::GroupConsumerHandle;

#[test]
fn capture_borrows_the_unique_handle_and_exposes_stable_rejection() {
    fn capture(
        handle: &mut GroupConsumerHandle,
    ) -> Result<GroupConsumerSeekCapture<'_>, GroupConsumerSeekAdmissionError> {
        handle.capture_seek()
    }
    fn inspect(error: GroupConsumerSeekAdmissionError) -> GroupConsumerSeekAdmissionErrorKind {
        error.kind()
    }

    let _ = capture;
    let _ = inspect;
}
