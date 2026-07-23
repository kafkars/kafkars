//! Public admin thread-safety and builder scenarios.

use super::Admin;

#[test]
fn admin_is_a_shared_thread_safe_handle() {
    fn assert_shared<T: Clone + Send + Sync>() {}
    assert_shared::<Admin>();
}
