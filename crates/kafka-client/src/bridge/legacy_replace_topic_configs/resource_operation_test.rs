//! Private generic legacy replacement observer type scenarios.

use std::future::Future;

use super::resource_operation::AdminLegacyReplaceConfigResources;

#[test]
fn private_generic_operation_is_send_without_runtime_dependencies() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<AdminLegacyReplaceConfigResources>();
}
