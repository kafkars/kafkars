//! Reconciliation claim-prefix acceptance and lossless rejection evidence.

use std::sync::Arc;

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedTopicPartition, AssignmentEpoch,
    FetchFailure, FetchFence, InstallResolvedAssignment, Moment, NextFetchOffset, PartitionIndex,
    ReconcileResolvedAssignment, ResolvedAssignedPartition, ResolvedAssignmentTarget, TopicId,
};

use super::{AssignedConsumerEventStore, AssignedConsumerEventStoreError};

#[test]
fn reconciliation_claims_accept_control_prefix_before_assignment_starts() {
    let (mut machine, mut store, old_epoch, _old_fences) = assigned_three();
    let reconciled = reconcile(&mut machine, old_epoch);
    assert!(matches!(
        reconciled.effects(),
        [
            AssignedConsumerEffect::Revoke { partition: first, .. },
            AssignedConsumerEffect::Revoke { partition: second, .. },
            AssignedConsumerEffect::Suspend { fence: retained },
            AssignedConsumerEffect::FetchReady { fence: retained_start, .. },
            AssignedConsumerEffect::FetchReady { fence: acquired_start, .. },
        ] if *first == partition(0)
            && *second == partition(2)
            && retained.partition() == partition(1)
            && retained_start.position().partition() == partition(1)
            && acquired_start.position().partition() == partition(3)
    ));

    store
        .prepare_reconciliation(2)
        .unwrap_or_else(|error| panic!("prepare reconciliation: {error:?}"))
        .commit_event_claims(reconciled.effects())
        .unwrap_or_else(|error| panic!("commit reconciliation: {error:?}"));
    assert_eq!(store.retained(), (2, 0));

    for fence in fetch_fences(reconciled.effects()) {
        store
            .retain_terminal(
                Arc::from("orders"),
                AssignedConsumerEffect::FetchFailed {
                    fence,
                    failure: FetchFailure::Transport,
                },
            )
            .unwrap_or_else(|(error, _topic)| panic!("retain reconciled claim: {error:?}"));
    }
    assert_eq!(store.retained(), (0, 2));
}

#[test]
fn failed_reconciliation_commit_preserves_every_prior_claim_losslessly() {
    let (mut machine, mut store, old_epoch, old_fences) = assigned_three();
    let reconciled = reconcile(&mut machine, old_epoch);
    let invalid = [
        reconciled.effects()[3],
        reconciled.effects()[0],
        reconciled.effects()[4],
    ];

    let error = store
        .prepare_reconciliation(2)
        .unwrap_or_else(|error| panic!("prepare invalid reconciliation: {error:?}"))
        .commit_event_claims(&invalid);
    assert_eq!(
        error,
        Err(AssignedConsumerEventStoreError::TransitionMismatch)
    );
    assert_eq!(store.retained(), (3, 0));

    for fence in old_fences {
        store
            .retain_terminal(
                Arc::from("orders"),
                AssignedConsumerEffect::FetchFailed {
                    fence,
                    failure: FetchFailure::Transport,
                },
            )
            .unwrap_or_else(|(error, _topic)| panic!("retain prior claim: {error:?}"));
    }
    assert_eq!(store.retained(), (0, 3));
}

fn assigned_three() -> (
    AssignedConsumerMachine,
    AssignedConsumerEventStore,
    AssignmentEpoch,
    Vec<FetchFence>,
) {
    let mut machine = AssignedConsumerMachine::new();
    let assigned = machine
        .install_resolved_assignment(InstallResolvedAssignment::new(
            None,
            vec![resolved(0, 10), resolved(1, 20), resolved(2, 30)],
            Moment::from_tick(0),
            0,
        ))
        .unwrap_or_else(|error| panic!("install assignment: {error}"));
    let epoch = assigned
        .assignment_epoch()
        .unwrap_or_else(|| panic!("assignment epoch"));
    let fences = fetch_fences(assigned.effects());
    let mut store = event_store(3);
    store
        .prepare_replacement(3)
        .unwrap_or_else(|error| panic!("prepare assignment: {error:?}"))
        .commit_event_claims(assigned.effects())
        .unwrap_or_else(|error| panic!("commit assignment: {error:?}"));
    (machine, store, epoch, fences)
}

fn reconcile(
    machine: &mut AssignedConsumerMachine,
    old_epoch: AssignmentEpoch,
) -> kafka_client_core::AssignedConsumerTransition {
    machine
        .reconcile_resolved_assignment(ReconcileResolvedAssignment::new(
            old_epoch,
            vec![
                ResolvedAssignmentTarget::Retain(partition(1)),
                ResolvedAssignmentTarget::Acquire(resolved(3, 40)),
            ],
            Moment::from_tick(1),
            0,
        ))
        .unwrap_or_else(|error| panic!("reconcile assignment: {error}"))
}

fn fetch_fences(effects: &[AssignedConsumerEffect]) -> Vec<FetchFence> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            AssignedConsumerEffect::FetchReady { fence, .. } => Some(*fence),
            _ => None,
        })
        .collect()
}

fn resolved(index: u32, next_offset: i64) -> ResolvedAssignedPartition {
    ResolvedAssignedPartition::new(partition(index), offset(next_offset))
}

fn partition(index: u32) -> AssignedTopicPartition {
    AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(index))
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}

fn event_store(capacity: usize) -> AssignedConsumerEventStore {
    AssignedConsumerEventStore::new(capacity)
        .unwrap_or_else(|error| panic!("event store: {error:?}"))
}
