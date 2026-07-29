//! Producer-fencing builder shape and call-boundary deadline tests.

use std::time::{Duration, Instant};

use super::{
    FenceProducersBuilder, FenceProducersResult,
    builder::{CallDeadline, CallDeadlineError},
};

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn builder_and_result_are_send_sync_without_runtime_types() {
    assert_send_sync::<FenceProducersBuilder>();
    assert_send_sync::<FenceProducersResult>();
}

#[test]
fn collection_delay_and_replacement_timeout_remain_call_anchored() {
    let boundary = Instant::now();
    let deadline = CallDeadline::from_boundary(boundary, Duration::from_millis(10));

    assert_eq!(
        deadline.remaining_at(boundary + Duration::from_millis(4)),
        Ok(Duration::from_millis(6))
    );
    assert_eq!(
        deadline
            .with_timeout(Duration::from_millis(3))
            .remaining_at(boundary + Duration::from_millis(3)),
        Err(CallDeadlineError::Elapsed)
    );
}
