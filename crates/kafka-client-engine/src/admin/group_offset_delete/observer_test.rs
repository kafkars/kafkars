//! Named linear offset-deletion observer shape scenarios.

#[test]
fn observer_is_send_and_remains_a_small_named_surface() {
    fn assert_send<T: Send>() {}
    assert_send::<super::DeleteConsumerGroupOffsetsObserver>();
    assert!(
        std::mem::size_of::<super::DeleteConsumerGroupOffsetsObserver>()
            <= 4 * std::mem::size_of::<usize>()
    );
}
