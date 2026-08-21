//! Configuration-resource operation ownership surface tests.

use super::ListConfigResources;

#[test]
fn named_operation_has_a_stable_debug_identity() {
    fn assert_debug<T: std::fmt::Debug>() {}
    assert_debug::<ListConfigResources>();
}
