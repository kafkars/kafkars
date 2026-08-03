//! Close-port classification and bounded-admission vocabulary.

use std::sync::Arc;

use kafka_client_core::MembershipCycle;

use crate::clock::MonotonicClock;

use super::classic_group_entry_fault::ClassicGroupEntryFault;
use super::registry_close::GroupRegistryCloseError;
use super::registry_close_port::{GroupConsumerCloseObservation, GroupConsumerClosePortError};
use super::registry_shard::{GroupConsumerShardLockError, GroupConsumerShardOwner};
use super::registry_test_support::{deadline, register, started_registry, stop_registry};
use super::registry_wake::{GroupConsumerShardWake, GroupConsumerShardWakeError};

#[test]
fn close_port_keeps_contention_group_state_and_terminal_state_distinct() {
    assert_ne!(
        GroupConsumerClosePortError::Lock(GroupConsumerShardLockError::Contended),
        GroupConsumerClosePortError::Registry(GroupRegistryCloseError::AlreadyClosing)
    );
    assert_ne!(
        GroupConsumerCloseObservation::Pending,
        GroupConsumerCloseObservation::Complete
    );
    assert_ne!(
        GroupConsumerCloseObservation::Faulted,
        GroupConsumerCloseObservation::NotAccepted
    );
}

#[test]
fn accepted_close_observation_reports_a_retained_entry_fault_instead_of_pending_forever() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let authority = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered group"))
        .close_authority();
    let completion = registry
        .close_group_explicit(group_id, deadline(100), &authority)
        .unwrap_or_else(|error| panic!("accepted close: {error:?}"));
    registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("closing group"))
        .fault = Some(ClassicGroupEntryFault::SyncRecoverySemantic(
        MembershipCycle::try_from_raw(9).unwrap_or_else(|| panic!("nonzero membership cycle")),
    ));
    let (owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(MonotonicClock::new()),
        Arc::new(NoopWake),
    );

    assert_eq!(
        port.observe_close(group_id),
        Ok(GroupConsumerCloseObservation::Faulted)
    );

    let mut registry = owner.terminal_registry();
    let _fault = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .and_then(|entry| entry.fault.take())
        .unwrap_or_else(|| panic!("retained fault"));
    stop_registry(&mut registry);
    assert!(matches!(
        completion.observe(),
        super::classic_group_leave::GroupConsumerCloseCompletionObservation::Terminal(
            super::classic_group_leave::GroupConsumerCloseTerminal::Succeeded
        )
    ));
}

struct NoopWake;

impl GroupConsumerShardWake for NoopWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        Ok(())
    }
}
