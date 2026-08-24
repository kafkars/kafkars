//! Direct Heartbeat normalization-to-core policy scenarios.

use kafka_client_core::{ClassicBrokerStage, ClassicGroupFatalReason, ClassicGroupPhase, Moment};

use crate::driver::classic_group::{
    install_heartbeat_broker_rejection_terminal, install_heartbeat_deadline_terminal,
};

use super::{
    classic_group_heartbeat::{ClassicHeartbeatExecutionState, ClassicHeartbeatSuccessor},
    classic_group_heartbeat_interpret::interpret_heartbeat,
    classic_group_heartbeat_settlement_test::{
        heartbeat_calls, make_driver_owned, prepared_heartbeat,
    },
    registry_test_support::stop_registry,
};

#[test]
fn generation_rejection_revokes_and_arms_the_exact_retained_rejoin() {
    let (mut registry, group_id, key) = prepared_heartbeat();
    make_driver_owned(&mut registry, group_id, key);
    install_heartbeat_broker_rejection_terminal(heartbeat_calls(&mut registry), key, 25);
    let successor = interpret_rejection(&mut registry, group_id, key);

    assert!(matches!(successor, ClassicHeartbeatSuccessor::Dormant));
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert_eq!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::WaitingToRejoin
    );
    assert_eq!(
        entry.rejoin.schedule(),
        entry.classic.machine().pending_rejoin()
    );
    assert!(entry.catalog.live_assignment().is_some());
    assert!(!entry.revocation.is_dormant());

    confirm_terminal(&mut registry, group_id);
    stop_registry(&mut registry);
}

#[test]
fn unknown_heartbeat_code_revokes_and_becomes_the_exact_core_fatal() {
    let (mut registry, group_id, key) = prepared_heartbeat();
    make_driver_owned(&mut registry, group_id, key);
    install_heartbeat_broker_rejection_terminal(heartbeat_calls(&mut registry), key, 1_236);
    let successor = interpret_rejection(&mut registry, group_id, key);

    assert!(matches!(successor, ClassicHeartbeatSuccessor::Dormant));
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    let fatal = entry
        .classic
        .machine()
        .fatal()
        .unwrap_or_else(|| panic!("fatal broker fact expected"));
    let ClassicGroupFatalReason::Broker { stage, error } = fatal.reason() else {
        panic!("broker fatal expected");
    };
    assert_eq!(stage, ClassicBrokerStage::Heartbeat);
    assert_eq!(error.code(), 1_236);
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Fatal);
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.rejoin.is_dormant());

    confirm_terminal(&mut registry, group_id);
    stop_registry(&mut registry);
}

#[test]
fn coordinator_deadline_after_the_old_attempt_starts_a_fresh_bounded_rejoin() {
    let (mut registry, group_id, key) = prepared_heartbeat();
    make_driver_owned(&mut registry, group_id, key);
    install_heartbeat_deadline_terminal(heartbeat_calls(&mut registry), key);
    let (entries, calls) = (&mut registry.entries, &mut registry.heartbeat_calls);
    let entry = entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    let terminal = calls
        .as_mut()
        .unwrap_or_else(|| panic!("Heartbeat calls expected"))
        .begin_classic_heartbeat_settlement(
            entry
                .heartbeat
                .accepted()
                .unwrap_or_else(|| panic!("accepted Heartbeat expected")),
        )
        .unwrap_or_else(|error| panic!("Heartbeat settlement failed: {error:?}"));

    let successor = interpret_heartbeat(
        entry,
        Moment::from_tick(key.deadline().core().tick() + 1),
        &terminal,
        true,
    )
    .unwrap_or_else(|_error| panic!("coordinator-loss interpretation failed"));
    assert!(matches!(successor, ClassicHeartbeatSuccessor::Dormant));
    assert_eq!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::WaitingToRejoin
    );
    assert_eq!(
        entry.rejoin.schedule(),
        entry.classic.machine().pending_rejoin()
    );
    assert!(entry.rediscovery.awaits_route_transfer());
    assert!(!entry.revocation.is_dormant());

    drop(terminal);
    confirm_terminal(&mut registry, group_id);
    registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"))
        .rediscovery
        .clear_rediscovery_after_driver_shutdown();
    stop_registry(&mut registry);
}

fn interpret_rejection(
    registry: &mut super::registry::GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
    key: crate::driver::classic_group::ClassicHeartbeatCallKey,
) -> ClassicHeartbeatSuccessor {
    let (entries, calls) = (&mut registry.entries, &mut registry.heartbeat_calls);
    let entry = entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    let calls = calls
        .as_mut()
        .unwrap_or_else(|| panic!("Heartbeat calls expected"));
    let accepted = entry
        .heartbeat
        .accepted()
        .unwrap_or_else(|| panic!("accepted Heartbeat expected"));
    let terminal = calls
        .begin_classic_heartbeat_settlement(accepted)
        .unwrap_or_else(|error| panic!("Heartbeat settlement failed: {error:?}"));
    let now = Moment::from_tick(key.deadline().core().tick() - 1);
    let successor = interpret_heartbeat(entry, now, &terminal, false).unwrap_or_else(|error| match error {
        super::classic_group_heartbeat_interpret::ClassicHeartbeatInterpretationFailure::PostCoreRejection(
            fault,
        ) => panic!("broker rejection interpretation failed: {:?}", fault.failure()),
        _ => panic!("broker rejection interpretation failed"),
    });
    drop(terminal);
    successor
}

fn confirm_terminal(
    registry: &mut super::registry::GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
) {
    let (entries, calls) = (&mut registry.entries, &mut registry.heartbeat_calls);
    let entry = entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    let calls = calls
        .as_mut()
        .unwrap_or_else(|| panic!("Heartbeat calls expected"));
    let state = entry
        .heartbeat
        .replace(ClassicHeartbeatExecutionState::Dormant);
    let ClassicHeartbeatExecutionState::DriverOwned(owner) = state else {
        panic!("driver-owned Heartbeat expected");
    };
    calls
        .confirm_classic_heartbeat_settlement(owner.into_accepted())
        .unwrap_or_else(|_failure| panic!("exact confirmation must succeed"));
}
