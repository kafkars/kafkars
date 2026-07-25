//! Direct Heartbeat normalization-to-core policy scenarios.

use kafka_client_core::{ClassicGroupPhase, Moment};

use crate::driver::classic_group::install_heartbeat_broker_rejection_terminal;

use super::{
    classic_group_heartbeat::{ClassicHeartbeatExecutionState, ClassicHeartbeatSuccessor},
    classic_group_heartbeat_interpret::interpret_heartbeat,
    classic_group_heartbeat_settlement_test::{
        heartbeat_calls, make_driver_owned, prepared_heartbeat,
    },
    registry_test_support::stop_registry,
};

#[test]
fn exact_broker_rejection_becomes_conservative_core_loss() {
    let (mut registry, group_id, key) = prepared_heartbeat();
    make_driver_owned(&mut registry, group_id, key);
    install_heartbeat_broker_rejection_terminal(heartbeat_calls(&mut registry), key, 25);
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

    assert!(matches!(
        interpret_heartbeat(entry, now, &terminal),
        Ok(ClassicHeartbeatSuccessor::Dormant)
    ));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.catalog.live_assignment().is_none());

    drop(terminal);
    let state = entry
        .heartbeat
        .replace(ClassicHeartbeatExecutionState::Dormant);
    let ClassicHeartbeatExecutionState::DriverOwned(owner) = state else {
        panic!("driver-owned Heartbeat expected");
    };
    calls
        .confirm_classic_heartbeat_settlement(owner.into_accepted())
        .unwrap_or_else(|_failure| panic!("exact confirmation must succeed"));
    stop_registry(&mut registry);
}
