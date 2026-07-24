//! Deliberately creates an unbound direct-Fetch executor.

struct DirectFetchExecutor;
struct FetchAttemptDeadline;

impl DirectFetchExecutor {
    fn create_unbound() -> Self {
        Self
    }
}

impl FetchAttemptDeadline {
    fn capture_for_fetch() -> Self {
        Self
    }
}

fn bypass_machine_binding() {
    let _executor = DirectFetchExecutor::create_unbound();
    let _deadline = FetchAttemptDeadline::capture_for_fetch();
}
