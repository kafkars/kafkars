//! Named legacy full-snapshot configuration observer shape scenarios.

#[test]
fn observer_is_a_named_linear_engine_surface() {
    fn assert_send<T: Send>() {}
    assert_send::<super::LegacyAlterConfigsObserver>();
    assert!(
        std::mem::size_of::<super::LegacyAlterConfigsObserver>()
            <= 4 * std::mem::size_of::<usize>()
    );
}
