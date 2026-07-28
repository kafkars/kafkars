//! Linear state ownership and exact fence retention scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId,
    GroupPositionBootstrapEffect, GroupPositionBootstrapInput, GroupPositionBootstrapMachine,
    GroupPositionFence, MemberId, MembershipCycle, Moment, PartitionIndex, TopicId,
};

use crate::{
    clock::OperationDeadline,
    driver::{GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchKey},
    protocol::consumer::{
        GroupOffsetFetchPreparation, GroupOffsetFetchTopic, prepare_group_offset_fetch_request,
    },
};

use super::{
    ClassicGroupPositionCompleted, ClassicGroupPositionConfirmationPending,
    ClassicGroupPositionDriverOwned, ClassicGroupPositionExecution,
    ClassicGroupPositionExecutionState, ClassicGroupPositionHandoff, ClassicGroupPositionPrepared,
};

#[test]
fn every_position_stage_retains_one_exact_linear_owner() {
    let fence = position_fence();
    let deadline = operation_deadline();
    let (machine, correlation, request, key, result_buffer) = prepared_parts(deadline);
    let mut execution = ClassicGroupPositionExecution::new();
    assert!(execution.is_dormant());
    assert_eq!(execution.unsettled(), 0);

    execution.set(ClassicGroupPositionExecutionState::Prepared(
        ClassicGroupPositionPrepared::new(key, machine, correlation, request, result_buffer),
    ));
    let ClassicGroupPositionExecutionState::Prepared(prepared) =
        execution.replace(ClassicGroupPositionExecutionState::Dormant)
    else {
        panic!("prepared state expected");
    };
    assert_eq!(prepared.key().fence(), fence);
    assert_eq!(prepared.key().operation_deadline(), deadline);
    let (key, machine, correlation, request, result_buffer) = prepared.into_parts();

    execution.set(ClassicGroupPositionExecutionState::Handoff(
        ClassicGroupPositionHandoff::new(machine, correlation, result_buffer),
    ));
    let ClassicGroupPositionExecutionState::Handoff(handoff) =
        execution.replace(ClassicGroupPositionExecutionState::Dormant)
    else {
        panic!("handoff state expected");
    };
    assert_eq!(handoff.fence(), fence);
    let (machine, correlation, result_buffer) = handoff.into_parts();
    drop((key, request));

    let accepted = GroupPositionOffsetFetchAccepted::from_fence_for_test(fence);
    execution.set(ClassicGroupPositionExecutionState::DriverOwned(
        ClassicGroupPositionDriverOwned::new(machine, correlation, accepted, result_buffer),
    ));
    let ClassicGroupPositionExecutionState::DriverOwned(driver_owned) =
        execution.replace(ClassicGroupPositionExecutionState::Dormant)
    else {
        panic!("driver-owned state expected");
    };
    assert_eq!(driver_owned.fence(), fence);
    assert_eq!(driver_owned.accepted().fence(), fence);
    assert_eq!(execution.unsettled(), 0);
}

#[test]
fn confirmation_pending_keeps_applied_terminal_until_exact_confirmation() {
    let fence = position_fence();
    let completed = completed();
    let accepted = GroupPositionOffsetFetchAccepted::from_fence_for_test(fence);
    let pending = ClassicGroupPositionConfirmationPending::new(completed, accepted);
    assert_eq!(pending.fence(), fence);
    assert_eq!(pending.accepted().fence(), fence);

    let (completed, accepted) = pending.into_parts();
    assert_eq!(accepted.fence(), fence);
    assert_eq!(completed.fence(), fence);
    assert_eq!(completed.observed_at(), Moment::from_tick(1));
    assert!(matches!(
        completed.terminal(),
        kafka_client_core::GroupPositionBootstrapTerminal::Ready(batch)
            if batch.facts().is_empty()
    ));

    let mut execution = ClassicGroupPositionExecution::new();
    execution.set(ClassicGroupPositionExecutionState::Complete(completed));
    assert_eq!(execution.unsettled(), 1);
    assert!(matches!(
        execution.state(),
        ClassicGroupPositionExecutionState::Complete(owner) if owner.fence() == fence
    ));
    let ClassicGroupPositionExecutionState::Complete(completed) =
        execution.replace(ClassicGroupPositionExecutionState::Dormant)
    else {
        panic!("complete state expected");
    };
    let (machine, terminal, observed_at, _operation_deadline) = completed.into_parts();
    assert_eq!(machine.fence(), fence);
    assert_eq!(observed_at, Moment::from_tick(1));
    assert!(matches!(
        terminal,
        kafka_client_core::GroupPositionBootstrapTerminal::Ready(batch)
            if batch.facts().is_empty()
    ));
}

fn prepared_parts(
    deadline: OperationDeadline,
) -> (
    GroupPositionBootstrapMachine,
    crate::protocol::consumer::GroupOffsetFetchCorrelation,
    crate::protocol::consumer::PreparedGroupOffsetFetchRequest,
    GroupPositionOffsetFetchKey,
    Vec<kafka_client_core::GroupPositionPartitionFact>,
) {
    let fence = position_fence();
    let partition =
        GroupAssignmentPartition::new(TopicId::from_raw(3), PartitionIndex::from_raw(0));
    let mut machine =
        GroupPositionBootstrapMachine::try_new(fence, deadline.core(), vec![partition])
            .unwrap_or_else(|error| panic!("position machine: {error}"));
    let transition = machine
        .apply(GroupPositionBootstrapInput::Start {
            fence,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("position start: {error}"));
    assert!(matches!(
        transition.into_effect(),
        Some(GroupPositionBootstrapEffect::FetchOffsets { .. })
    ));
    let GroupOffsetFetchPreparation::Prepared(prepared) = prepare_group_offset_fetch_request(
        Arc::from("readers"),
        vec![GroupOffsetFetchTopic::new(Arc::from("orders"), vec![0])],
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("protocol preparation: {error:?}")) else {
        panic!("nonempty assignment must prepare");
    };
    let (correlation, request) = prepared.into_parts();
    (
        machine,
        correlation,
        request,
        GroupPositionOffsetFetchKey::new(fence, deadline),
        Vec::with_capacity(1),
    )
}

fn completed() -> ClassicGroupPositionCompleted {
    let fence = position_fence();
    let mut machine =
        GroupPositionBootstrapMachine::try_new(fence, Deadline::from_tick(20), Vec::new())
            .unwrap_or_else(|error| panic!("empty position machine: {error}"));
    let transition = machine
        .apply(GroupPositionBootstrapInput::Start {
            fence,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("empty position start: {error}"));
    let Some(GroupPositionBootstrapEffect::Complete { terminal, .. }) = transition.into_effect()
    else {
        panic!("empty position must complete locally");
    };
    ClassicGroupPositionCompleted::new(machine, terminal, Moment::from_tick(1))
}

fn position_fence() -> GroupPositionFence {
    GroupPositionFence::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        MembershipCycle::initial(),
        MemberId::try_from_raw(2).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(3).unwrap_or_else(|| panic!("generation")),
    )
}

fn operation_deadline() -> OperationDeadline {
    OperationDeadline::from_core_for_test(Deadline::from_tick(20))
}
