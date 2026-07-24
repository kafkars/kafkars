//! Deliberately invokes every tracked-Fetch owner method from a foreign module.

struct TrackedFetchCalls;

impl TrackedFetchCalls {
    fn invoke_every_protected_method(&mut self) {
        self.try_submit_fetch();
        self.observe_fetch_control();
        self.poll_fetch();
        self.begin_fetch_settlement();
        self.confirm_fetch_settlement();
        self.restore_fetch_settlement();
        self.confirm_stale_fetch();
        self.recover_fetches_after_driver_shutdown();
    }
}
