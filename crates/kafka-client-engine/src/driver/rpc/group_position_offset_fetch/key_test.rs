//! Exact assignment fence and original deadline key scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::Deadline;

use super::{calls_test::fence, key::GroupPositionOffsetFetchKey};
use crate::clock::OperationDeadline;

#[test]
fn key_retains_the_exact_fence_and_both_original_deadline_representations() {
    let fence = fence(7);
    let transport = Instant::now() + Duration::from_secs(3);
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(41), transport);
    let key = GroupPositionOffsetFetchKey::new(fence, deadline);

    assert_eq!(key.fence(), fence);
    assert_eq!(key.operation_deadline().core(), Deadline::from_tick(41));
    assert_eq!(key.operation_deadline().transport(), transport);
}
