//! Compile-time resume capture surface contract.

use super::{GroupConsumerResumeCapture, GroupConsumerResumeCaptureError};
use crate::consumer::GroupConsumerHandle;

#[test]
fn resume_capture_borrows_the_handle_before_target_conversion() {
    fn capture(
        handle: &mut GroupConsumerHandle,
    ) -> Result<GroupConsumerResumeCapture<'_>, GroupConsumerResumeCaptureError> {
        handle.capture_resume()
    }

    let _ = capture;
}
