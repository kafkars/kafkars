//! Rediscovery terminal permission and terminal-failure gate scenarios.

use std::time::Duration;

use kafka_client_core::{ConsumerGroupHeartbeatFailure, GroupId, Moment};

use crate::clock::MonotonicClock;
use crate::consumer::GroupConsumerStartupFailureKind;
use crate::driver::TopicPartitionCountFact;
use crate::driver::classic_group::{
    ClassicCoordinatorInvalidationPermission, ClassicCoordinatorInvalidationTerminalFailure,
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    consumer_group_execution::ConsumerGroupRediscoveryState,
    consumer_group_execution_cadence::ConsumerGroupCoordinatorLoadRetryTurn,
    consumer_group_execution_fencing::consumer_group_execution_is_ready,
    consumer_group_execution_terminal::ConsumerGroupRediscoveryDecision,
    consumer_group_heartbeat_settlement_test::modern_entry_with_instance,
    registry_test_support::{register, started_registry, stop_registry},
};

#[test]
fn submitted_modern_invalidation_stays_blocked_until_its_terminal_permission() {
    for permission in [
        ClassicCoordinatorInvalidationPermission::Applied,
        ClassicCoordinatorInvalidationPermission::IgnoredStale,
    ] {
        let (mut registry, group_id) = modern_awaiting_registry(true);
        let execution = registry.entries[0]
            .consumer
            .as_ref()
            .unwrap_or_else(|| panic!("modern execution"));
        assert_eq!(
            execution.rediscovery_state(),
            ConsumerGroupRediscoveryState::AwaitingInvalidationAdmission
        );
        assert!(!consumer_group_execution_is_ready(execution));

        registry
            .apply_classic_coordinator_invalidation_terminal(group_id, Ok(permission))
            .unwrap_or_else(|error| panic!("terminal permission: {error:?}"));
        let execution = registry.entries[0]
            .consumer
            .as_ref()
            .unwrap_or_else(|| panic!("modern execution"));
        assert_eq!(
            execution.rediscovery_state(),
            ConsumerGroupRediscoveryState::ReplacementAdmitted
        );
        assert!(consumer_group_execution_is_ready(execution));
        drop(registry);
    }
}

#[test]
fn modern_invalidation_terminal_failure_becomes_a_startup_terminal() {
    let (mut registry, group_id) = modern_awaiting_registry(false);
    assert_eq!(
        registry.apply_classic_coordinator_invalidation_terminal(
            group_id,
            Err(ClassicCoordinatorInvalidationTerminalFailure::CapacityReached),
        ),
        Ok(())
    );
    assert_eq!(
        registry.consumer_group_startup_failure(group_id),
        Ok(Some(GroupConsumerStartupFailureKind::Execution))
    );
    drop(registry);
}

#[test]
fn applied_and_stale_terminals_are_the_only_fresh_join_permissions() {
    for permission in [
        ClassicCoordinatorInvalidationPermission::Applied,
        ClassicCoordinatorInvalidationPermission::IgnoredStale,
    ] {
        let (mut registry, group_id) = invalidating_registry();
        registry
            .apply_classic_coordinator_invalidation_terminal(group_id, Ok(permission))
            .unwrap_or_else(|error| panic!("terminal permission failed: {error:?}"));
        assert!(
            !registry
                .entry(group_id)
                .unwrap_or_else(|| panic!("entry expected"))
                .rediscovery
                .blocks_join()
        );
        stop_registry(&mut registry);
    }
}

#[test]
fn capacity_terminal_cannot_claim_that_the_consumed_token_survived() {
    let (mut registry, group_id) = invalidating_registry();

    assert_eq!(
        registry.apply_classic_coordinator_invalidation_terminal(
            group_id,
            Err(ClassicCoordinatorInvalidationTerminalFailure::CapacityReached),
        ),
        Err(ClassicGroupExecutionError::CoordinatorInvalidationTerminal)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert!(entry.rediscovery.blocks_join());
    assert!(matches!(
        &entry.fault,
        Some(ClassicGroupEntryFault::CoordinatorInvalidationTerminal(
            ClassicCoordinatorInvalidationTerminalFailure::CapacityReached
        ))
    ));
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    drop(entry.fault.take());
    entry.rediscovery.clear_rediscovery_after_driver_shutdown();
    stop_registry(&mut registry);
}

#[test]
fn a_foreign_group_terminal_cannot_open_another_groups_gate() {
    let (mut registry, group_id) = invalidating_registry();
    let foreign = GroupId::try_from_raw(group_id.get() + 1)
        .unwrap_or_else(|| panic!("foreign group identity expected"));

    assert_eq!(
        registry.apply_classic_coordinator_invalidation_terminal(
            foreign,
            Ok(ClassicCoordinatorInvalidationPermission::Applied),
        ),
        Err(ClassicGroupExecutionError::CallIdentityMismatch)
    );
    assert!(
        registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("entry expected"))
            .rediscovery
            .blocks_join()
    );
    registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"))
        .rediscovery
        .clear_rediscovery_after_driver_shutdown();
    stop_registry(&mut registry);
}

pub(super) fn invalidating_registry() -> (super::registry::GroupConsumerRegistry, GroupId) {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let rediscovery = &mut registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"))
        .rediscovery;
    rediscovery
        .prepare_rediscovery_install()
        .unwrap_or_else(|error| panic!("rediscovery install failed: {error:?}"))
        .commit();
    rediscovery
        .confirm_rediscovery_transfer()
        .unwrap_or_else(|error| panic!("route transfer failed: {error:?}"));
    (registry, group_id)
}

fn modern_awaiting_registry(retry_due: bool) -> (super::registry::GroupConsumerRegistry, GroupId) {
    let mut entry = modern_entry_with_instance(None);
    let group_id = entry.group_id();
    let topic_id = entry
        .catalog
        .topic_id("orders")
        .unwrap_or_else(|| panic!("topic identity"));
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let execution = entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"));
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    execution
        .topic_identities_mut()
        .append(
            topic_id,
            TopicPartitionCountFact {
                metadata_generation: 1,
                logical_partition_count: 1,
                kafka_topic_id: Some([7; 16]),
            },
        )
        .unwrap_or_else(|error| panic!("topic identity: {error:?}"));
    assert_eq!(
        execution
            .apply_current_rediscovery(capture.now(), ConsumerGroupHeartbeatFailure::Broker(16),),
        Ok(ConsumerGroupRediscoveryDecision::Rediscover)
    );
    if retry_due {
        let schedule = execution
            .machine()
            .retry_schedule()
            .unwrap_or_else(|| panic!("rediscovery retry schedule"));
        assert_eq!(
            execution.prepare_due_coordinator_load_retry(Moment::from_tick(
                schedule.not_before().tick(),
            )),
            Ok(ConsumerGroupCoordinatorLoadRetryTurn::SubmissionReady)
        );
    }
    let mut registry = started_registry();
    registry.entries.push(entry);
    (registry, group_id)
}
