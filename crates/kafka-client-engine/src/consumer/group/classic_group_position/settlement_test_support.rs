//! Shared exact-owner fixtures for position terminal settlement tests.

use kafka_client_core::{GroupId, GroupPositionFence, Moment};

pub(super) use crate::driver::GroupPositionOffsetFetchTestPartition as PartitionValue;
use crate::{
    clock::OperationDeadline,
    driver::{
        GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchCompletionFailureKind,
        GroupPositionOffsetFetchDriverFailureKind, GroupPositionOffsetFetchKey,
    },
};

use super::{
    super::{
        classic_group_sync_settlement::ClassicGroupSyncSettlementTurn,
        classic_group_sync_settlement_test::install_assignment_terminal,
        classic_group_sync_submission_test::{make_sync_driver_owned, prepared_registry},
        registry::GroupConsumerRegistry,
    },
    ClassicGroupPositionExecutionState,
};

pub(super) struct PositionSettlementFixture {
    pub(super) registry: GroupConsumerRegistry,
    pub(super) group_id: GroupId,
    pub(super) fence: GroupPositionFence,
    pub(super) deadline: OperationDeadline,
}

pub(super) fn driver_owned_fixture(partitions: &[i32]) -> PositionSettlementFixture {
    let mut fixture = prepared_fixture(partitions);
    let entry = fixture
        .registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    let (key, request) = entry
        .position
        .begin_handoff()
        .unwrap_or_else(|error| panic!("position handoff: {error:?}"));
    drop(request);
    entry
        .position
        .confirm_driver_owned(GroupPositionOffsetFetchAccepted::from_fence_for_test(
            key.fence(),
        ))
        .unwrap_or_else(|_failure| panic!("position driver ownership"));
    fixture
}

pub(super) fn prepared_fixture(partitions: &[i32]) -> PositionSettlementFixture {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", partitions);
    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(3)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(4)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    let ClassicGroupPositionExecutionState::Prepared(prepared) = entry.position.state() else {
        panic!("prepared position expected");
    };
    let fence = prepared.key().fence();
    let deadline = prepared.key().operation_deadline();
    PositionSettlementFixture {
        registry,
        group_id,
        fence,
        deadline,
    }
}

pub(super) fn install_legacy_terminal(
    fixture: &mut PositionSettlementFixture,
    selected_version: Option<i16>,
    throttle_time_ms: i32,
    group_error: i16,
    values: &[(i32, PartitionValue)],
) {
    fixture
        .registry
        .position_calls
        .as_mut()
        .unwrap_or_else(|| panic!("position calls expected"))
        .install_legacy_terminal_for_test(
            GroupPositionOffsetFetchKey::new(fixture.fence, fixture.deadline),
            selected_version,
            throttle_time_ms,
            group_error,
            values,
        );
}

pub(super) fn install_empty_terminal(
    fixture: &mut PositionSettlementFixture,
    selected_version: Option<i16>,
) {
    fixture
        .registry
        .position_calls
        .as_mut()
        .unwrap_or_else(|| panic!("position calls expected"))
        .install_empty_terminal_for_test(
            GroupPositionOffsetFetchKey::new(fixture.fence, fixture.deadline),
            selected_version,
        );
}

pub(super) fn install_driver_failure(
    fixture: &mut PositionSettlementFixture,
    kind: GroupPositionOffsetFetchDriverFailureKind,
) {
    fixture
        .registry
        .position_calls
        .as_mut()
        .unwrap_or_else(|| panic!("position calls expected"))
        .install_driver_failure_kind_for_test(
            GroupPositionOffsetFetchKey::new(fixture.fence, fixture.deadline),
            kind,
        );
}

pub(super) fn install_completion_failure(
    fixture: &mut PositionSettlementFixture,
    deadline: OperationDeadline,
    kind: GroupPositionOffsetFetchCompletionFailureKind,
) {
    fixture
        .registry
        .position_calls
        .as_mut()
        .unwrap_or_else(|| panic!("position calls expected"))
        .install_completion_failure_kind_for_test(
            GroupPositionOffsetFetchKey::new(fixture.fence, deadline),
            kind,
        );
}

pub(super) fn position_state(
    fixture: &PositionSettlementFixture,
) -> &ClassicGroupPositionExecutionState {
    fixture
        .registry
        .entry(fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"))
        .position
        .state()
}

pub(super) fn release_restored_owners(fixture: &mut PositionSettlementFixture) {
    let mut calls = fixture
        .registry
        .position_calls
        .take()
        .unwrap_or_else(|| panic!("position calls expected"));
    let mut recovery = calls.recover_group_position_offset_fetches_after_driver_shutdown();
    drop(
        recovery
            .take_settled()
            .unwrap_or_else(|| panic!("restored raw terminal expected")),
    );
    let entry = fixture
        .registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    drop(
        entry
            .position
            .replace(ClassicGroupPositionExecutionState::Dormant),
    );
}
