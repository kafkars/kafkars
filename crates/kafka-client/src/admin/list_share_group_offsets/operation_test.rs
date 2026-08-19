//! Named `ShareGroup` offset-listing operation surface tests.

use super::ListShareGroupOffsets;

#[test]
fn operation_is_send_and_debug_without_clone() {
    fn assert_send_debug<T: Send + std::fmt::Debug>() {}

    assert_send_debug::<ListShareGroupOffsets>();
}
