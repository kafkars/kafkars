//! Named `DeleteTopics` observer shape scenarios.

#[test]
fn observer_is_a_named_linear_engine_surface() {
    fn assert_send<T: Send>() {}
    assert_send::<super::DeleteTopicsObserver>();
    assert!(std::mem::size_of::<super::DeleteTopicsObserver>() <= 4 * std::mem::size_of::<usize>());
}
