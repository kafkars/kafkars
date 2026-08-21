//! Multi-ShareGroup result value guarantees.

use std::time::Duration;

use crate::admin::BatchResult;

use super::ListShareGroupsOffsetsResult;

#[test]
fn result_preserves_aggregate_throttle_and_empty_ordered_batch() {
    let result =
        ListShareGroupsOffsetsResult::new(Duration::from_millis(23), BatchResult::new(Vec::new()));
    assert_eq!(result.throttle_time(), Duration::from_millis(23));
    assert!(result.groups().entries().is_empty());
}
