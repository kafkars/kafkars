//! Named linear offset-alteration observer shape scenarios.

#[test]
fn observer_is_send_and_remains_a_small_named_surface() {
    fn assert_send<T: Send>() {}
    assert_send::<super::AlterConsumerGroupOffsetsObserver>();
    assert!(
        std::mem::size_of::<super::AlterConsumerGroupOffsetsObserver>()
            <= 4 * std::mem::size_of::<usize>()
    );
}
