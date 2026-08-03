//! Dormant KIP-848 explicit-close completion evidence.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind, GroupId,
    GroupPositionMissingOffsetPolicy, Moment, ReadIsolation,
};

use super::{
    classic_group_leave::{
        GroupConsumerCloseCompletion, GroupConsumerCloseCompletionObservation,
        GroupConsumerCloseTerminal,
    },
    classic_group_position::submission_test::{driver, shutdown_driver},
    consumer_group_close::complete_consumer_group_leave,
    consumer_group_heartbeat_settlement::{ConsumerGroupHeartbeatSettlementTurn, settle_success},
    consumer_group_heartbeat_settlement_test::{installed_modern_entry, success_with},
    registry::GroupConsumerRegistry,
    registry_entry::{
        GroupConsumerEntry, GroupConsumerEntryState, default_classic_processing_lease_policy,
    },
    registry_membership::GroupConsumerMembershipTurn,
    registry_test_support::deadline,
};
use crate::{
    clock::MonotonicClock,
    consumer::group_registration_request::{GroupConsumerClassicAssignor, GroupConsumerProtocol},
};

#[test]
fn dormant_modern_close_completes_without_submitting_a_leave() {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"));
    let mut entry = GroupConsumerEntry::try_new_with_protocol_configuration(
        group_id,
        &Arc::from("workers"),
        None,
        &[Arc::from("orders")],
        GroupConsumerProtocol::Consumer,
        GroupConsumerClassicAssignor::Range,
        super::classic_group_test_support::timing(),
        super::classic_group_test_support::heartbeat_policy(),
        super::classic_group_test_support::rejoin_policy(),
        GroupPositionMissingOffsetPolicy::Error,
        ReadIsolation::ReadUncommitted,
        default_classic_processing_lease_policy(),
    )
    .unwrap_or_else(|error| panic!("entry: {error:?}"));
    let completion = Arc::new(GroupConsumerCloseCompletion::pending());
    entry
        .leave
        .begin(deadline(20), Arc::clone(&completion))
        .unwrap_or_else(|_completion| panic!("close admission"));
    entry.state = GroupConsumerEntryState::Closing;
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);

    assert_eq!(
        registry.turn_one_consumer_group_close(Moment::from_tick(10)),
        Ok(super::consumer_group_close::ConsumerGroupCloseTurn::Progress)
    );
    let entry = registry
        .entries
        .first_mut()
        .unwrap_or_else(|| panic!("entry"));
    assert!(entry.leave.publish_terminal());
    assert_eq!(
        completion.observe(),
        GroupConsumerCloseCompletionObservation::Terminal(GroupConsumerCloseTerminal::Succeeded)
    );
}

#[test]
fn control_requested_modern_close_prepares_the_existing_epoch_minus_one_leave() {
    let clock = MonotonicClock::new();
    let (entry, _topic_id) = installed_modern_entry();
    let authority = entry.close_authority();
    let close_deadline = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("close deadline: {error}"))
        .operation_deadline();
    assert!(authority.request(close_deadline));
    assert!(!authority.request(close_deadline));
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);

    assert!(
        registry
            .close_one_requested_group()
            .unwrap_or_else(|error| panic!("apply control shutdown: {error:?}"))
    );
    let now = clock
        .now()
        .unwrap_or_else(|error| panic!("close turn time: {error}"));
    assert_eq!(
        registry.turn_one_consumer_group_close(now),
        Ok(super::consumer_group_close::ConsumerGroupCloseTurn::Progress)
    );
    let entry = registry
        .entries
        .first()
        .unwrap_or_else(|| panic!("closing modern entry"));
    let prepared = entry
        .consumer
        .as_ref()
        .and_then(|execution| execution.prepared())
        .unwrap_or_else(|| panic!("prepared KIP-848 leave"));
    assert_eq!(prepared.kind(), ConsumerGroupHeartbeatRequestKind::Leave);
    assert_eq!(prepared.deadline(), close_deadline);
    assert_eq!(
        entry
            .consumer
            .as_ref()
            .map(|execution| execution.machine().phase()),
        Some(ConsumerGroupHeartbeatPhase::Leaving)
    );
}

#[test]
fn close_during_reconciliation_prepares_leave_instead_of_empty_owned_ack() {
    let clock = MonotonicClock::new();
    let (mut entry, _topic_id) = installed_modern_entry();
    let schedule = entry
        .consumer
        .as_ref()
        .and_then(|execution| execution.machine().schedule())
        .unwrap_or_else(|| panic!("steady schedule"));
    let now = Moment::from_tick(schedule.deadline().tick());
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .prepare_due_heartbeat(now, &clock)
        .unwrap_or_else(|error| panic!("prepare replacement heartbeat: {error:?}"));
    assert_eq!(
        settle_success(&mut entry, now, success_with(2, 1)),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
    let completion = Arc::new(GroupConsumerCloseCompletion::pending());
    let close_deadline = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("close deadline: {error}"))
        .operation_deadline();
    entry
        .leave
        .begin(close_deadline, Arc::clone(&completion))
        .unwrap_or_else(|_completion| panic!("close admission"));
    entry.state = GroupConsumerEntryState::Closing;
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);
    let mut driver = driver();

    assert_eq!(
        registry.turn_membership(now, &clock, &driver),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    assert_eq!(
        registry.turn_membership(now, &clock, &driver),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    let entry = registry
        .entries
        .first_mut()
        .unwrap_or_else(|| panic!("closing entry"));
    let execution = entry
        .consumer
        .as_ref()
        .unwrap_or_else(|| panic!("modern execution"));
    let prepared = execution
        .prepared()
        .unwrap_or_else(|| panic!("prepared leave"));
    assert_eq!(prepared.kind(), ConsumerGroupHeartbeatRequestKind::Leave);
    assert_eq!(
        execution.machine().phase(),
        ConsumerGroupHeartbeatPhase::Leaving
    );
    assert!(prepared.assignment_generation().is_some());
    assert!(entry.consumer_reconciliation.is_some());
    assert!(entry.catalog.live_assignment().is_some());
    complete_consumer_group_leave(entry)
        .unwrap_or_else(|error| panic!("complete reconciliation leave: {error:?}"));
    assert!(entry.consumer_reconciliation.is_none());
    assert!(entry.consumer_revocation.is_some());
    assert_eq!(
        entry
            .consumer
            .as_ref()
            .map(|execution| execution.machine().phase()),
        Some(ConsumerGroupHeartbeatPhase::Closed)
    );

    shutdown_driver(&mut driver);
}
