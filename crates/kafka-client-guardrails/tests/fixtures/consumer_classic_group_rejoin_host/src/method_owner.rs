//! Configured owner proving nonempty classic rejoin call allowlists are exact.

fn execute<T>(owner: T) {
    owner.prepare_one_classic_rejoin();
    owner.prepare_rejoin_install();
    owner.clear_rejoin_exact();
    owner.stage_rejoin_join();
}
