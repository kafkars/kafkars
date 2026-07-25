//! Deadline, two-phase driver settlement, and shutdown scenarios.

use kafka_client_core::{
    DeliveryStatus, GroupOffsetCommitFailureKind, GroupOffsetCommitTerminal, Moment,
};

use super::{
    host::{GroupOffsetCommitHost, GroupOffsetCommitTurn},
    test_support::{catalog, checkpoint, deadline, driver},
};

#[test]
fn deadline_before_driver_is_not_sent_and_publishes_once() {
    let catalog = catalog();
    let checkpoint = checkpoint(&catalog);
    let mut host = GroupOffsetCommitHost::start_group_offset_commit_host()
        .unwrap_or_else(|error| panic!("host start: {error}"));
    let admission = host
        .try_admit(&catalog, deadline(5), checkpoint)
        .unwrap_or_else(|failure| panic!("admission failed: {:?}", failure.kind));
    assert_eq!(host.next_deadline(), Some(deadline(5).core()));
    let driver = driver();

    assert_eq!(
        host.turn(Moment::from_tick(5), &driver),
        Ok(GroupOffsetCommitTurn::Progress)
    );
    let terminal = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("terminal: {error}"));
    let GroupOffsetCommitTerminal::Failed(failure) = terminal else {
        panic!("deadline failure expected");
    };
    assert_eq!(
        failure.kind(),
        GroupOffsetCommitFailureKind::DeadlineElapsed
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);

    host.close_admission();
    let _ = host.turn(Moment::from_tick(5), &driver);
    let join = host
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
