//! Dormant and joining modern owners never invent a cadence heartbeat.

use std::time::Duration;

use kafka_client_core::{GroupId, Moment};

use crate::{clock::MonotonicClock, driver::GroupPositionOffsetFetchAccepted};

use super::{
    classic_group_position::submission_test::{driver, shutdown_driver},
    consumer_group_execution::ConsumerGroupExecution,
    consumer_group_heartbeat_settlement::{ConsumerGroupHeartbeatSettlementTurn, settle_success},
    consumer_group_heartbeat_settlement_test::{installed_modern_entry, success_with},
    consumer_group_heartbeat_submission::prepare_request,
    registry::GroupConsumerRegistry,
    registry_membership::GroupConsumerMembershipTurn,
};

#[test]
fn joining_owner_has_no_cadence_to_prepare() {
    let clock = MonotonicClock::new();
    let mut execution =
        ConsumerGroupExecution::new(GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id")));
    let capture = clock
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    assert!(
        !execution
            .prepare_due_heartbeat(Moment::from_tick(u64::MAX), &clock)
            .unwrap_or_else(|error| panic!("prepare: {error:?}"))
    );
}

#[test]
fn blocked_reconciliation_retirement_does_not_suppress_old_owned_cadence() {
    let clock = MonotonicClock::new();
    let (mut entry, _topic_id) = installed_modern_entry();
    let first_schedule = entry
        .consumer
        .as_ref()
        .and_then(|execution| execution.machine().schedule())
        .unwrap_or_else(|| panic!("first steady schedule"));
    let first_now = Moment::from_tick(first_schedule.deadline().tick());
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .prepare_due_heartbeat(first_now, &clock)
        .unwrap_or_else(|error| panic!("prepare replacement heartbeat: {error:?}"));
    assert_eq!(
        settle_success(&mut entry, first_now, success_with(2, 1)),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
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
    let reconciliation_schedule = entry
        .consumer
        .as_ref()
        .and_then(|execution| execution.machine().schedule())
        .unwrap_or_else(|| panic!("reconciliation cadence"));
    let due = Moment::from_tick(reconciliation_schedule.deadline().tick());
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);
    let mut driver = driver();

    assert_eq!(
        registry.turn_membership(due, &clock, &driver),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    let entry = registry
        .entries
        .first()
        .unwrap_or_else(|| panic!("reconciling entry"));
    let prepared = entry
        .consumer
        .as_ref()
        .and_then(ConsumerGroupExecution::prepared)
        .unwrap_or_else(|| panic!("prepared reconciling heartbeat"));
    assert_eq!(
        prepared
            .member_epoch()
            .map(kafka_client_core::ConsumerGroupMemberEpoch::get),
        Some(2)
    );
    assert_eq!(
        prepared
            .assignment_generation()
            .map(kafka_client_core::AssignmentGeneration::get),
        Some(1)
    );
    let request = prepare_request(entry)
        .unwrap_or_else(|()| panic!("old-owned heartbeat request"))
        .into_generated_request();
    assert_eq!(request.member_epoch, 2);
    let owned = request
        .topic_partitions
        .as_ref()
        .unwrap_or_else(|| panic!("old owned assignment"));
    assert_eq!(owned.len(), 1);
    assert_eq!(owned[0].partitions, [0]);

    shutdown_driver(&mut driver);
}
