//! Follower and leader Join settlement plus close-fenced Sync scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{ClassicGroupPhase, GroupId, Moment};

use crate::{
    EngineConfig,
    clock::MonotonicClock,
    driver::{
        DriverOwner,
        classic_group::{
            AcceptedJoinGroupCall, JoinGroupCallKey, JoinGroupPoll,
            install_empty_leader_join_terminal, install_follower_join_terminal,
            install_leader_join_terminal,
        },
    },
};

use super::{
    classic_group_join::{
        ClassicGroupExecutionState, ClassicGroupJoinIdentity, ClassicGroupJoinSuccessor,
    },
    classic_group_join_settlement::ClassicGroupJoinSettlementTurn,
    classic_group_sync_submission::ClassicGroupSyncSubmissionTurn,
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntryState,
    registry_test_support::{started_registry, stop_registry},
};

#[test]
fn follower_terminal_stages_then_confirms_the_exact_prepared_sync() {
    let (mut registry, group_id, identity) = follower_join_terminal();
    let key = join_key(identity);

    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(1)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    let first_entry = entry(&registry, group_id);
    assert_eq!(
        first_entry.classic.machine().phase(),
        ClassicGroupPhase::Syncing
    );
    assert!(first_entry.classic.pending().is_some());
    assert!(matches!(
        first_entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::JoinConfirmationPending {
            call,
            successor: ClassicGroupJoinSuccessor::Sync(prepared),
        } if call.identity() == identity
            && prepared.group_id() == identity.group_id()
            && prepared.cycle().get() == identity.cycle().get()
            && prepared.deadline() == identity.deadline()
    ));
    assert_eq!(
        registry
            .join_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Join calls expected"))
            .poll_join_group(),
        Ok(JoinGroupPoll::ConfirmationPending { key })
    );

    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(2)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    let entry = entry(&registry, group_id);
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::PreparedSync(prepared)
            if prepared.group_id() == identity.group_id()
                && prepared.cycle().get() == identity.cycle().get()
                && prepared.deadline() == identity.deadline()
    ));
    assert_eq!(
        registry
            .join_calls
            .as_ref()
            .unwrap_or_else(|| panic!("Join calls expected"))
            .retained_join_group_count(),
        0
    );
    stop_registry(&mut registry);
}

#[test]
#[expect(
    clippy::maybe_infinite_iter,
    reason = "the flagged cycle calls return fixed scalar fences and perform no iteration"
)]
fn leader_terminal_stages_then_confirms_exact_partition_count_read() {
    let (mut registry, group_id, identity) = leader_join_terminal();
    let key = join_key(identity);

    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(1)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    let staged_entry = entry(&registry, group_id);
    assert_eq!(
        staged_entry.classic.machine().phase(),
        ClassicGroupPhase::AwaitingPartitionCounts
    );
    assert!(staged_entry.classic.pending().is_some());
    assert!(matches!(
        staged_entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::JoinConfirmationPending {
            call,
            successor: ClassicGroupJoinSuccessor::PartitionCounts(prepared),
        } if call.identity() == identity
            && prepared.cycle() == identity.cycle()
            && prepared.deadline() == identity.deadline()
    ));
    assert_eq!(
        registry
            .join_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Join calls expected"))
            .poll_join_group(),
        Ok(JoinGroupPoll::ConfirmationPending { key })
    );

    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(2)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    let prepared_entry = entry(&registry, group_id);
    assert!(matches!(
        prepared_entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::PreparedPartitionCounts(prepared)
            if prepared.cycle() == identity.cycle()
                && prepared.deadline() == identity.deadline()
    ));
    assert_eq!(
        registry
            .join_calls
            .as_ref()
            .unwrap_or_else(|| panic!("Join calls expected"))
            .retained_join_group_count(),
        0
    );
    stop_registry(&mut registry);
}

#[test]
fn close_after_join_success_never_submits_the_prepared_sync() {
    let (mut registry, group_id, identity) = follower_join_terminal();
    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(1)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    registry
        .close_group(group_id)
        .unwrap_or_else(|error| panic!("group close failed: {error:?}"));
    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(2)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );

    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver build failed: {error}"));
    assert_eq!(
        registry.submit_one_classic_sync(&driver),
        Ok(ClassicGroupSyncSubmissionTurn::Idle)
    );
    let entry = entry(&registry, group_id);
    assert_eq!(entry.state, GroupConsumerEntryState::Closing);
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::PreparedSync(prepared)
            if prepared.group_id() == identity.group_id()
                && prepared.cycle().get() == identity.cycle().get()
    ));
    assert_eq!(
        registry
            .sync_calls
            .as_ref()
            .unwrap_or_else(|| panic!("Sync calls expected"))
            .retained_sync_group_count(),
        0
    );
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown failed: {error}"));
    stop_registry(&mut registry);
}

pub(super) fn follower_join_terminal() -> (GroupConsumerRegistry, GroupId, ClassicGroupJoinIdentity)
{
    let (mut registry, group_id, identity) = prepared_join_terminal();
    install_follower_join_terminal(
        registry
            .join_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Join calls expected")),
        join_key(identity),
    );
    (registry, group_id, identity)
}

pub(super) fn leader_join_terminal() -> (GroupConsumerRegistry, GroupId, ClassicGroupJoinIdentity) {
    let (mut registry, group_id, identity) = prepared_join_terminal();
    install_leader_join_terminal(
        registry
            .join_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Join calls expected")),
        join_key(identity),
    );
    (registry, group_id, identity)
}

pub(super) fn empty_leader_join_terminal()
-> (GroupConsumerRegistry, GroupId, ClassicGroupJoinIdentity) {
    let (mut registry, group_id, identity) = prepared_join_terminal_with_topics(Vec::new());
    install_empty_leader_join_terminal(
        registry
            .join_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Join calls expected")),
        join_key(identity),
    );
    (registry, group_id, identity)
}

pub(super) fn prepared_join_terminal() -> (GroupConsumerRegistry, GroupId, ClassicGroupJoinIdentity)
{
    prepared_join_terminal_with_topics(vec![Arc::from("orders")])
}

fn prepared_join_terminal_with_topics(
    local_topics: Vec<Arc<str>>,
) -> (GroupConsumerRegistry, GroupId, ClassicGroupJoinIdentity) {
    let mut registry = started_registry();
    let group_id = registry
        .try_register(
            Arc::from("workers"),
            local_topics,
            super::classic_group_test_support::timing(),
            super::classic_group_test_support::heartbeat_policy(),
            super::classic_group_test_support::rejoin_policy(),
        )
        .unwrap_or_else(|failure| panic!("registration failed: {:?}", failure.kind));
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(7))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    entry
        .execution
        .begin(&mut entry.classic, capture)
        .unwrap_or_else(|error| panic!("classic begin failed: {error:?}"));
    let handoff = entry
        .execution
        .begin_join_handoff()
        .unwrap_or_else(|error| panic!("Join handoff failed: {error:?}"));
    let identity = handoff.identity();
    let key = join_key(identity);
    entry
        .execution
        .confirm_join_driver_owned(
            handoff.into_driver_acceptance(),
            AcceptedJoinGroupCall::from_key_for_test(key),
        )
        .unwrap_or_else(|_failure| panic!("Join ownership failed"));
    (registry, group_id, identity)
}

pub(super) fn entry(
    registry: &GroupConsumerRegistry,
    group_id: GroupId,
) -> &super::registry_entry::GroupConsumerEntry {
    registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"))
}

pub(super) fn join_key(identity: ClassicGroupJoinIdentity) -> JoinGroupCallKey {
    JoinGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline())
}
