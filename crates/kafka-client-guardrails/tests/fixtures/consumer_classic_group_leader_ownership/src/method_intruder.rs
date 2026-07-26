//! Deliberate foreign leader core-transition calls.

fn intrude(owner: &mut Owner) {
    owner.apply_leader_join();
    owner.apply_leader_partition_counts();
    owner.prepared_partition_counts();
    owner.begin_partition_count_handoff();
    owner.restore_partition_count_handoff();
    owner.confirm_partition_count_driver_owned();
    owner.fail_prepared_partition_counts();
    owner.complete_partition_counts();
    owner.recover_partition_count_after_driver_shutdown();
    owner.recover_classic_partition_counts_after_driver_shutdown();
    owner.settle_one_classic_partition_count();
    owner.submit_one_classic_partition_count();
}

struct Owner;

impl Owner {
    fn apply_leader_join(&mut self) {}

    fn apply_leader_partition_counts(&mut self) {}

    fn prepared_partition_counts(&mut self) {}

    fn begin_partition_count_handoff(&mut self) {}

    fn restore_partition_count_handoff(&mut self) {}

    fn confirm_partition_count_driver_owned(&mut self) {}

    fn fail_prepared_partition_counts(&mut self) {}

    fn complete_partition_counts(&mut self) {}

    fn recover_partition_count_after_driver_shutdown(&mut self) {}

    fn recover_classic_partition_counts_after_driver_shutdown(&mut self) {}

    fn settle_one_classic_partition_count(&mut self) {}

    fn submit_one_classic_partition_count(&mut self) {}
}
