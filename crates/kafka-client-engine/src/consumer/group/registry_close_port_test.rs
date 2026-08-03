//! Close-port classification and bounded-admission vocabulary.

use std::sync::Arc;

use kafka_client_core::{GroupPositionMissingOffsetPolicy, MembershipCycle, Moment};

use crate::{clock::MonotonicClock, consumer::GroupConsumerPositionFailureKind};

use super::classic_group_entry_fault::ClassicGroupEntryFault;
use super::classic_group_leave::GroupConsumerCloseCompletionObservation;
use super::classic_group_position::{
    ClassicGroupPositionSettlementTurn,
    settlement_test_support::{
        PartitionValue, driver_owned_fixture_with_policy, install_legacy_terminal,
    },
};
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

#[test]
fn accepted_close_keeps_a_position_failure_pending_and_retained() {
    let mut fixture =
        driver_owned_fixture_with_policy(&[0], GroupPositionMissingOffsetPolicy::Error);
    install_legacy_terminal(&mut fixture, Some(7), 0, 0, &[(0, PartitionValue::Missing)]);
    for _turn in 0..2 {
        assert_eq!(
            fixture
                .registry
                .settle_one_classic_group_position(Moment::from_tick(50)),
            Ok(ClassicGroupPositionSettlementTurn::Progress)
        );
    }
    assert!(
        fixture
            .registry
            .terminalize_one_classic_group_position_failure()
    );
    let group_id = fixture.group_id;
    let authority = fixture
        .registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("position-faulted group"))
        .close_authority();
    let completion = fixture
        .registry
        .close_group_explicit(group_id, deadline(100), &authority)
        .unwrap_or_else(|error| panic!("accepted position-fault close: {error:?}"));
    let (mut owner, port) = GroupConsumerShardOwner::new(
        fixture.registry,
        Arc::new(MonotonicClock::new()),
        Arc::new(NoopWake),
    );

    assert_eq!(
        port.observe_close(group_id),
        Ok(GroupConsumerCloseObservation::Pending)
    );
    {
        let registry = owner.lock_registry_for_test();
        let entry = registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("retained closing entry"));
        assert_eq!(
            entry.position_failure_observation,
            Some(GroupConsumerPositionFailureKind::MissingOffset)
        );
        assert!(matches!(
            &entry.fault,
            Some(ClassicGroupEntryFault::PositionFailure(_))
        ));
    }

    let mut registry = owner.terminal_registry();
    let _fault = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .and_then(|entry| entry.fault.take())
        .unwrap_or_else(|| panic!("retained position fault"));
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    let commit_join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry finish: {error}"));
    drop(registry);
    let recv_join = owner
        .stop_recv_notifier()
        .unwrap_or_else(|| panic!("receive notifier"));
    commit_join
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("commit notifier join: {error}"));
    recv_join
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("receive notifier join: {error}"));
    assert!(matches!(
        completion.observe(),
        GroupConsumerCloseCompletionObservation::Terminal(_)
    ));
}

struct NoopWake;

impl GroupConsumerShardWake for NoopWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        Ok(())
    }
}
