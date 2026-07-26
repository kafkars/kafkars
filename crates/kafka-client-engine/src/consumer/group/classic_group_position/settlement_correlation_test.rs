//! Pre-core protocol/core correlation failure with exact raw restoration.

use std::sync::Arc;

use kafka_client_core::Moment;

use super::{
    super::{
        classic_group_execution::ClassicGroupExecutionError, registry_test_support::stop_registry,
    },
    ClassicGroupPositionDriverOwned, ClassicGroupPositionExecutionError,
    ClassicGroupPositionExecutionState,
    settlement_test_support::{
        PartitionValue, driver_owned_fixture, install_legacy_terminal, position_state,
        release_restored_owners,
    },
};

use crate::protocol::consumer::{
    GroupOffsetFetchPreparation, GroupOffsetFetchTopic, prepare_group_offset_fetch_request,
};

#[test]
fn protocol_core_correlation_mismatch_restores_raw_and_driver_owned() {
    let mut fixture = driver_owned_fixture(&[0]);
    replace_with_two_partition_correlation(&mut fixture);
    install_legacy_terminal(
        &mut fixture,
        Some(7),
        0,
        0,
        &[
            (0, PartitionValue::Committed(4)),
            (1, PartitionValue::Committed(5)),
        ],
    );

    assert_eq!(
        fixture
            .registry
            .settle_one_classic_group_position(Moment::from_tick(50)),
        Err(ClassicGroupExecutionError::Position(
            ClassicGroupPositionExecutionError::TerminalCorrelation
        ))
    );
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::DriverOwned(owner)
            if owner.accepted().fence() == fixture.fence
    ));
    assert!(matches!(
        fixture
            .registry
            .position_calls
            .as_mut()
            .unwrap_or_else(|| panic!("position calls expected"))
            .poll_group_position_offset_fetch(),
        Ok(crate::driver::GroupPositionOffsetFetchPoll::TerminalReady { fence })
            if fence == fixture.fence
    ));
    release_restored_owners(&mut fixture);
    stop_registry(&mut fixture.registry);
}

fn replace_with_two_partition_correlation(
    fixture: &mut super::settlement_test_support::PositionSettlementFixture,
) {
    let GroupOffsetFetchPreparation::Prepared(prepared) = prepare_group_offset_fetch_request(
        Arc::from("workers"),
        vec![GroupOffsetFetchTopic::new(Arc::from("orders"), vec![0, 1])],
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("corrupt correlation preparation: {error:?}")) else {
        panic!("two partitions require a request");
    };
    let (correlation, request) = prepared.into_parts();
    drop(request);
    let entry = fixture
        .registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    let state = entry
        .position
        .replace(ClassicGroupPositionExecutionState::Dormant);
    let ClassicGroupPositionExecutionState::DriverOwned(owner) = state else {
        panic!("driver-owned position expected");
    };
    let (machine, _original, accepted, result_buffer) = owner.into_parts();
    entry
        .position
        .set(ClassicGroupPositionExecutionState::DriverOwned(
            ClassicGroupPositionDriverOwned::new(machine, correlation, accepted, result_buffer),
        ));
}
