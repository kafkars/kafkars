//! Provenance-aware replay of retained terminal inputs after driver shutdown.

use kafka_client_core::{
    DeliveryStatus, GroupOffsetCommitFailureKind, GroupOffsetCommitInput, GroupOffsetCommitTerminal,
};

use super::{
    host::{
        GroupOffsetCommitHost, GroupOffsetCommitSettlementFault,
        GroupOffsetCommitSettlementProvenance,
    },
    test_support::{catalog, checkpoint, deadline},
};

#[test]
fn definitely_unsent_replay_retires_the_queued_submission_once() {
    let catalog = catalog();
    let mut host = GroupOffsetCommitHost::start_group_offset_commit_host()
        .unwrap_or_else(|error| panic!("host start: {error}"));
    let admission = host
        .try_admit(&catalog, deadline(9), checkpoint(&catalog))
        .unwrap_or_else(|failure| panic!("admission failed: {:?}", failure.kind));
    let operation_id = host.operations[0].operation_id;
    host.settlement_fault = Some(GroupOffsetCommitSettlementFault {
        operation_id,
        input: GroupOffsetCommitInput::DeadlineElapsed {
            delivery: DeliveryStatus::NotSent,
        },
        provenance: GroupOffsetCommitSettlementProvenance::DefinitelyUnsent,
    });

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recovery: {error}"));
    let terminal = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("terminal: {error}"));
    let GroupOffsetCommitTerminal::Failed(failure) = terminal else {
        panic!("deadline terminal expected");
    };
    assert_eq!(
        failure.kind(),
        GroupOffsetCommitFailureKind::DeadlineElapsed
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);

    let join = host
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
