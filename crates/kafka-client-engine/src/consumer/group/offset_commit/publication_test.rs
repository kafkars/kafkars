//! Terminal observation reclaim and exact byte release.

use kafka_client_core::Moment;

use super::{
    host::{GroupOffsetCommitHost, GroupOffsetCommitTurn},
    test_support::{catalog, checkpoint, deadline, driver},
};

#[test]
fn observed_terminal_reclaims_the_exact_operation_charge() {
    let catalog = catalog();
    let mut host = GroupOffsetCommitHost::start_group_offset_commit_host()
        .unwrap_or_else(|error| panic!("host start: {error}"));
    let admission = host
        .try_admit(&catalog, deadline(5), checkpoint(&catalog))
        .unwrap_or_else(|failure| panic!("admission failed: {:?}", failure.kind));
    let driver = driver();
    assert_eq!(
        host.turn(Moment::from_tick(5), &driver),
        Ok(GroupOffsetCommitTurn::Progress)
    );
    let _terminal = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("terminal: {error}"));

    assert_eq!(
        host.turn(Moment::from_tick(5), &driver),
        Ok(GroupOffsetCommitTurn::Progress)
    );
    assert_eq!(host.retained_bytes_for_test(), 0);

    host.close_admission();
    let join = host
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
