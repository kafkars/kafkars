//! Deliberately creates an unbound direct-Fetch executor.

struct DirectFetchExecutor;

impl DirectFetchExecutor {
    fn create_unbound() -> Self {
        Self
    }
}

fn bypass_machine_binding() {
    let _executor = DirectFetchExecutor::create_unbound();
}
