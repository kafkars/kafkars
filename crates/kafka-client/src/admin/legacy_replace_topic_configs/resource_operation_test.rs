//! Generic destructive replacement operation type scenarios.

use std::future::Future;

use super::LegacyReplaceConfigResources;

#[test]
fn generic_replacement_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<LegacyReplaceConfigResources>();
}
