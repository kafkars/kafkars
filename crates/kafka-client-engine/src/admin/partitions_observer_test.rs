//! Named `CreatePartitions` observer shape scenarios.

#[test]
fn observer_is_a_named_linear_engine_surface() {
    fn assert_send<T: Send>() {}
    assert_send::<super::CreatePartitionsObserver>();
    assert!(
        std::mem::size_of::<super::CreatePartitionsObserver>() <= 4 * std::mem::size_of::<usize>()
    );
}
