//! Public builder inertness and submission-shape tests.

use std::time::Duration;

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn builder_and_result_shapes_are_thread_safe_without_runtime_types() {
    assert_send_sync::<super::DescribeConsumerGroupsBuilder>();
    assert_send_sync::<super::DescribeConsumerGroupsResult>();
    let _timeout = Duration::from_millis(1);
}
