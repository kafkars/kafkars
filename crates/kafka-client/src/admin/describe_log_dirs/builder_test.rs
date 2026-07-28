//! Public builder and result thread-safety shape tests.

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn builder_and_result_are_send_sync_without_runtime_types() {
    assert_send_sync::<super::DescribeLogDirsBuilder>();
    assert_send_sync::<super::DescribeLogDirsResult>();
}
