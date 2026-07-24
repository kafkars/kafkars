//! Named incremental configuration observer shape scenarios.

#[test]
fn observer_is_a_named_linear_engine_surface() {
    fn assert_send<T: Send>() {}
    assert_send::<super::IncrementalAlterConfigsObserver>();
    assert!(
        std::mem::size_of::<super::IncrementalAlterConfigsObserver>()
            <= 4 * std::mem::size_of::<usize>()
    );
}
