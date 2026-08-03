//! Execution-unavailable settlement scenarios after deterministic admission.

use kafka_client_core::{
    DeliveryStatus, GroupOffsetCommitFailureKind, GroupOffsetCommitMachine,
    GroupOffsetCommitTerminal, OperationId,
};

use crate::{
    consumer::GroupConsumerProtocol,
    protocol::consumer::{
        GroupOffsetCommitEntryReservation, GroupOffsetCommitResultReservation,
        PreparedGroupOffsetCommit,
    },
};

use super::{
    host::{
        GROUP_OFFSET_COMMIT_OPERATION_BYTES, GroupOffsetCommitHost, GroupOffsetCommitHostError,
    },
    preparation::{HostPreparation, PreparationOutcome},
    test_support::{catalog, checkpoint, deadline},
};

#[test]
fn execution_unavailable_is_definitely_unsent_and_terminal() {
    let catalog = catalog();
    let operation_id = OperationId::from_raw(7);
    let checkpoint = checkpoint(&catalog);
    let snapshot = GroupOffsetCommitHost::snapshot(
        GroupConsumerProtocol::Classic,
        &catalog,
        &checkpoint,
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("snapshot: {error}"));
    let operation_deadline = deadline(40);
    let admission = GroupOffsetCommitMachine::try_admit(
        operation_id,
        operation_deadline.core(),
        catalog.live_assignment(),
        checkpoint,
    )
    .unwrap_or_else(|error| panic!("core admission: {error}"));
    let (mut machine, effect) = admission.into_parts();
    let prepared = PreparedGroupOffsetCommit::from_effect(
        effect,
        operation_deadline,
        snapshot.session,
        snapshot.topic_names,
        GroupOffsetCommitEntryReservation::try_new(1)
            .unwrap_or_else(|error| panic!("entry reservation: {error:?}")),
        GroupOffsetCommitResultReservation::try_new(1)
            .unwrap_or_else(|error| panic!("result reservation: {error:?}")),
    )
    .unwrap_or_else(|error| panic!("prepared: {:?}", error.kind()));
    let mut host = GroupOffsetCommitHost::start_group_offset_commit_host()
        .unwrap_or_else(|error| panic!("host start: {error}"));

    let outcome = host.settle_preparation_failure(
        operation_id,
        &mut machine,
        HostPreparation::Ready {
            prepared,
            request: snapshot.request,
        },
        GroupOffsetCommitHostError::Preparation,
    );
    let PreparationOutcome::Installed(installed) = outcome else {
        panic!("execution terminal must install");
    };
    assert!(installed.attempt.is_none());
    assert_eq!(installed.byte_charge, GROUP_OFFSET_COMMIT_OPERATION_BYTES);
    assert_eq!(
        installed.fault,
        Some(GroupOffsetCommitHostError::Preparation)
    );
    let terminal = installed
        .terminal
        .unwrap_or_else(|| panic!("execution terminal"));
    let GroupOffsetCommitTerminal::Failed(failure) = terminal else {
        panic!("execution failure expected");
    };
    assert_eq!(
        failure.kind(),
        GroupOffsetCommitFailureKind::ExecutionUnavailable
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);

    host.close_admission();
    let join = host
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
