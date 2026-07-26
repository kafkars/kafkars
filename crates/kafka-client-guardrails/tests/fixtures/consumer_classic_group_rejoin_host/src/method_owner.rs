//! Configured owner proving nonempty classic rejoin call allowlists are exact.

fn execute<T>(owner: T) {
    owner.prepare_one_classic_rejoin();
    owner.prepare_rejoin_install();
    owner.clear_rejoin_exact();
    owner.stage_rejoin_join();
    owner.prepare_rediscovery_install();
    owner.confirm_rediscovery_transfer();
    owner.permit_rejoin();
    owner.clear_rediscovery_after_driver_shutdown();
}
