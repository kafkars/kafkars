//! Public leader-election builder and entry-point shape tests.

use std::time::Duration;

use crate::Admin;

use super::{ElectLeaders, ElectLeadersBuilder, LeaderElectionType};

#[test]
fn builder_and_all_partitions_entry_point_remain_inert() {
    let deadline: fn(ElectLeadersBuilder, Duration) -> ElectLeadersBuilder =
        ElectLeadersBuilder::deadline_after;
    let submit: fn(ElectLeadersBuilder) -> ElectLeaders = ElectLeadersBuilder::submit;
    let all: fn(&Admin, LeaderElectionType) -> ElectLeadersBuilder = Admin::elect_all_leaders;

    let _ = (deadline, submit, all);
}

#[test]
fn builder_is_send_without_runtime_types() {
    fn assert_send<T: Send>() {}

    assert_send::<ElectLeadersBuilder>();
}
