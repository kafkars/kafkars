//! Public builder shape and submission-boundary deadline tests.

use std::time::{Duration, Instant};

use super::{
    CreateAclsBuilder, CreateAclsResult,
    builder::{CallDeadlineError, SubmissionTimeout},
};

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn builder_and_result_are_send_sync_without_runtime_types() {
    assert_send_sync::<CreateAclsBuilder>();
    assert_send_sync::<CreateAclsResult>();
}

#[test]
fn inert_builder_delay_does_not_consume_the_submission_timeout() {
    let builder_construction = Instant::now();
    let submit_boundary = builder_construction + Duration::from_secs(60);
    let timeout = SubmissionTimeout::new(Duration::from_millis(10));

    assert_eq!(
        timeout.capture_at(submit_boundary),
        Ok(submit_boundary + Duration::from_millis(10))
    );
    assert_eq!(
        timeout
            .with_timeout(Duration::ZERO)
            .capture_at(submit_boundary),
        Err(CallDeadlineError::Elapsed)
    );
}
