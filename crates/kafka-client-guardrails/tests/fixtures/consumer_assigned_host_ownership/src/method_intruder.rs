//! Deliberately steals post-driver-shutdown release capabilities.

struct Intruder;

impl Intruder {
    fn violate(&self) {
        self.release_position_calls_after_driver_shutdown();
        self.release_fetch_executor_after_driver_shutdown();
        self.release_assigned_after_driver_shutdown();
        self.take_owner_for_post_driver_recovery();
        self.take_assigned_owner_after_driver_shutdown();
    }
}
