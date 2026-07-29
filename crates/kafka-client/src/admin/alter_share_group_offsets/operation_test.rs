//! Named ShareGroup offset-alteration operation ownership tests.

use super::AlterShareGroupOffsets;

#[test]
fn named_operation_is_send_and_has_a_stable_debug_identity() {
    fn assert_send_debug<T: Send + std::fmt::Debug>() {}
    assert_send_debug::<AlterShareGroupOffsets>();
}
