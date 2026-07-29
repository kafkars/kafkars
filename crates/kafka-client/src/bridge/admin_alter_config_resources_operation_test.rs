//! Private generic `IncrementalAlterConfigs` observer type scenarios.

use std::future::Future;

use super::admin_alter_config_resources_operation::AdminIncrementalAlterConfigResources;

#[test]
fn private_generic_operation_is_send_without_runtime_dependencies() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<AdminIncrementalAlterConfigResources>();
}
