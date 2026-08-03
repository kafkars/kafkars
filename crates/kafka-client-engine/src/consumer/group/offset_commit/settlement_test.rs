//! Two-phase driver settlement and exact terminal publication.

use kafka_client_core::{
    GroupOffsetCommitInput, GroupOffsetCommitPartitionOutcome, GroupOffsetCommitTerminal, Moment,
    PartitionIndex, TopicId,
};

use crate::consumer::GroupConsumerProtocol;

use super::{
    host::{GroupOffsetCommitHost, GroupOffsetCommitTurn},
    test_support::{catalog, checkpoint, deadline, driver},
};

#[test]
fn driver_terminal_is_applied_before_route_confirmation_and_publication() {
    let catalog = catalog();
    let mut host = GroupOffsetCommitHost::start_group_offset_commit_host()
        .unwrap_or_else(|error| panic!("host start: {error}"));
    let admission = host
        .try_admit(
            GroupConsumerProtocol::Classic,
            &catalog,
            deadline(50),
            checkpoint(&catalog),
        )
        .unwrap_or_else(|failure| panic!("admission failed: {:?}", failure.kind));
    let operation_id = host.operations[0].operation_id;
    host.install_accepted_terminal_for_test(
        operation_id,
        GroupOffsetCommitInput::BrokerResponded {
            throttle_time_ms: 3,
            outcomes: vec![GroupOffsetCommitPartitionOutcome::committed(
                TopicId::from_raw(1),
                PartitionIndex::from_raw(0),
            )],
        },
    )
    .unwrap_or_else(|error| panic!("terminal fixture: {error}"));
    assert_eq!(host.next_deadline(), Some(deadline(50).core()));
    let driver = driver();

    assert_eq!(
        host.turn(Moment::from_tick(10), &driver),
        Ok(GroupOffsetCommitTurn::Progress)
    );
    let terminal = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("terminal: {error}"));
    let GroupOffsetCommitTerminal::Committed(batch) = terminal else {
        panic!("committed batch expected");
    };
    assert_eq!(batch.throttle_time_ms(), 3);
    assert_eq!(batch.outcomes().len(), 1);

    host.close_admission();
    let _turn = host.turn(Moment::from_tick(10), &driver);
    let join = host
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

#[test]
fn original_deadline_escapes_coordinator_refresh_before_terminal_settlement() {
    let catalog = catalog();
    let mut host = GroupOffsetCommitHost::start_group_offset_commit_host()
        .unwrap_or_else(|error| panic!("host start: {error}"));
    let admission = host
        .try_admit(
            GroupConsumerProtocol::Classic,
            &catalog,
            deadline(50),
            checkpoint(&catalog),
        )
        .unwrap_or_else(|failure| panic!("admission failed: {:?}", failure.kind));
    let operation_id = host.operations[0].operation_id;
    host.install_accepted_terminal_for_test(
        operation_id,
        GroupOffsetCommitInput::BrokerResponded {
            throttle_time_ms: 3,
            outcomes: vec![GroupOffsetCommitPartitionOutcome::committed(
                TopicId::from_raw(1),
                PartitionIndex::from_raw(0),
            )],
        },
    )
    .unwrap_or_else(|error| panic!("terminal fixture: {error}"));
    let driver = driver();

    assert_eq!(
        host.turn(Moment::from_tick(50), &driver),
        Ok(GroupOffsetCommitTurn::Progress)
    );
    assert!(matches!(
        admission.observer.wait(),
        Ok(GroupOffsetCommitTerminal::Committed(_))
    ));

    host.close_admission();
    let _turn = host.turn(Moment::from_tick(50), &driver);
    let join = host
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
