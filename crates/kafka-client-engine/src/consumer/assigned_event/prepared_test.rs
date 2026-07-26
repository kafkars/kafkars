//! Pre-core reservation, exact commit, rollback, and no-start scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchFailure, FetchRecords, FetchThrottleFailure, Moment,
    NextFetchOffset, PartitionIndex, StartPosition, TopicId,
};

use super::{
    AssignedConsumerEventStore, AssignedConsumerEventStoreError, EventClaim, prepared::effect_claim,
};

#[test]
fn store_preallocates_fixed_claim_and_ready_capacity() {
    let store = event_store(7);

    assert_eq!(store.capacity, 7);
    assert!(store.claims.capacity() >= 7);
    assert!(store.ready.capacity() >= 7);
}

#[test]
fn prepared_claims_cover_position_and_fetch_throttle_starts() {
    let mut machine = AssignedConsumerMachine::new();
    let assigned = assign(
        &mut machine,
        vec![
            entry(0, StartPosition::Beginning),
            entry(1, StartPosition::Offset(offset(4))),
        ],
    );
    let AssignedConsumerEffect::ResolvePosition {
        fence: position, ..
    } = assigned.effects()[0]
    else {
        panic!("position fence");
    };
    let AssignedConsumerEffect::FetchReady { fence: fetch, .. } = assigned.effects()[1] else {
        panic!("fetch fence");
    };
    let starts = [
        AssignedConsumerEffect::ArmPositionThrottle {
            fence: position,
            deadline: Deadline::from_tick(20),
        },
        AssignedConsumerEffect::ArmFetchThrottle {
            fence: fetch,
            deadline: Deadline::from_tick(30),
        },
    ];
    assert_eq!(
        effect_claim(starts[0]),
        Some(EventClaim::Position(position))
    );
    assert_eq!(effect_claim(starts[1]), Some(EventClaim::Fetch(fetch)));

    let mut store = event_store(2);
    store
        .prepare_replacement(2)
        .unwrap_or_else(|error| panic!("prepare throttles: {error:?}"))
        .commit_event_claims(&starts)
        .unwrap_or_else(|error| panic!("commit throttles: {error:?}"));
    assert_eq!(store.retained(), (2, 0));
}

#[test]
fn arm_fetch_throttle_advances_live_fetch_claim() {
    let mut machine = AssignedConsumerMachine::new();
    let mut store = event_store(1);
    let assigned = assign(
        &mut machine,
        vec![entry(0, StartPosition::Offset(offset(1)))],
    );
    let first = assigned.effects()[0];
    store
        .prepare_replacement(1)
        .unwrap_or_else(|error| panic!("prepare fetch: {error:?}"))
        .commit_event_claims(assigned.effects())
        .unwrap_or_else(|error| panic!("commit fetch: {error:?}"));
    let AssignedConsumerEffect::FetchReady { fence, .. } = first else {
        panic!("first fetch");
    };
    let advanced = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(2),
            now: Moment::from_tick(10),
            throttle_ticks: 5,
        })
        .unwrap_or_else(|error| panic!("advance fetch: {error}"));
    let [arm @ AssignedConsumerEffect::ArmFetchThrottle { fence: next, .. }] = advanced.effects()
    else {
        panic!("next fetch throttle");
    };
    store
        .observe_effect(*arm)
        .unwrap_or_else(|error| panic!("advance throttle claim: {error:?}"));
    store
        .retain_terminal(
            Arc::from("orders"),
            AssignedConsumerEffect::FetchThrottleFailed {
                fence: *next,
                failure: FetchThrottleFailure::DeadlineOverflow,
            },
        )
        .unwrap_or_else(|(error, _topic)| panic!("retain throttle terminal: {error:?}"));
}

#[test]
fn prepared_replacement_rejects_impossible_terminal_and_hitchhiker_effects() {
    let mut machine = AssignedConsumerMachine::new();
    let assigned = assign(
        &mut machine,
        vec![entry(0, StartPosition::Offset(offset(1)))],
    );
    let fetch = match assigned.effects() {
        [AssignedConsumerEffect::FetchReady { fence, .. }] => *fence,
        effects => panic!("one fetch start, got {effects:?}"),
    };
    let mut store = event_store(1);

    let impossible = store
        .prepare_replacement(1)
        .unwrap_or_else(|error| panic!("prepare terminal: {error:?}"))
        .commit_event_claims(&[AssignedConsumerEffect::FetchFailed {
            fence: fetch,
            failure: FetchFailure::Transport,
        }]);
    assert_eq!(
        impossible,
        Err(AssignedConsumerEventStoreError::TransitionMismatch)
    );

    let hitchhiker = store
        .prepare_replacement(1)
        .unwrap_or_else(|error| panic!("prepare hitchhiker: {error:?}"))
        .commit_event_claims(&[
            AssignedConsumerEffect::Suspend {
                fence: fetch.position(),
            },
            assigned.effects()[0],
        ]);
    assert_eq!(
        hitchhiker,
        Err(AssignedConsumerEventStoreError::TransitionMismatch)
    );
    assert_eq!(store.retained(), (0, 0));
}

#[test]
fn no_start_resume_and_paused_seek_commit_without_inventing_claims() {
    let mut machine = AssignedConsumerMachine::new();
    let mut store = event_store(1);
    let assigned = assign(
        &mut machine,
        vec![entry(0, StartPosition::Offset(offset(1)))],
    );
    assert!(matches!(
        assigned.effects(),
        [AssignedConsumerEffect::FetchReady { .. }]
    ));
    let epoch = assigned
        .assignment_epoch()
        .unwrap_or_else(|| panic!("assignment epoch"));
    store
        .prepare_replacement(1)
        .unwrap_or_else(|error| panic!("prepare assignment: {error:?}"))
        .commit_event_claims(assigned.effects())
        .unwrap_or_else(|error| panic!("commit assignment: {error:?}"));
    let redundant = store
        .prepare_partition(partition(0))
        .unwrap_or_else(|error| panic!("prepare redundant resume: {error:?}"));
    let resumed = machine
        .apply(AssignedConsumerInput::Resume {
            assignment_epoch: epoch,
            partition: partition(0),
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("redundant resume: {error}"));
    assert!(resumed.effects().is_empty());
    redundant
        .commit_event_claims(resumed.effects())
        .unwrap_or_else(|error| panic!("commit redundant resume: {error:?}"));
    let paused = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: epoch,
            partition: partition(0),
        })
        .unwrap_or_else(|error| panic!("pause: {error}"));
    store
        .observe_effect(paused.effects()[0])
        .unwrap_or_else(|error| panic!("observe pause: {error:?}"));
    assert_eq!(store.retained(), (0, 0));

    let prepared = store
        .prepare_partition(partition(0))
        .unwrap_or_else(|error| panic!("prepare paused seek: {error:?}"));
    let sought = machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: epoch,
            partition: partition(0),
            position: StartPosition::Beginning,
            now: Moment::from_tick(2),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("paused seek: {error}"));
    assert!(matches!(
        sought.effects(),
        [AssignedConsumerEffect::Suspend { .. }]
    ));
    prepared
        .commit_event_claims(sought.effects())
        .unwrap_or_else(|error| panic!("commit paused seek: {error:?}"));
    store
        .observe_effect(sought.effects()[0])
        .unwrap_or_else(|error| panic!("observe paused seek: {error:?}"));
    assert_eq!(store.retained(), (0, 0));
}

#[test]
fn rejected_and_rolled_back_preparations_preserve_existing_claims() {
    let mut machine = AssignedConsumerMachine::new();
    let mut store = event_store(1);
    let assigned = assign(&mut machine, vec![entry(0, StartPosition::Beginning)]);
    let AssignedConsumerEffect::ResolvePosition { fence, .. } = assigned.effects()[0] else {
        panic!("position fence");
    };
    store
        .prepare_replacement(1)
        .unwrap_or_else(|error| panic!("prepare assignment: {error:?}"))
        .commit_event_claims(assigned.effects())
        .unwrap_or_else(|error| panic!("commit assignment: {error:?}"));
    store
        .prepare_partition(partition(0))
        .unwrap_or_else(|error| panic!("prepare rejected core input: {error:?}"))
        .rollback_event_claims();
    let terminal = AssignedConsumerEffect::PositionResolutionFailed {
        fence,
        failure: kafka_client_core::PositionResolutionFailure::Attempt(
            kafka_client_core::PositionResolutionAttemptFailure::Transport,
        ),
    };
    let error = store
        .prepare_replacement(1)
        .unwrap_or_else(|error| panic!("prepare invalid transition: {error:?}"))
        .commit_event_claims(&[terminal, terminal]);

    assert_eq!(
        error,
        Err(AssignedConsumerEventStoreError::TransitionMismatch)
    );
    assert_eq!(store.retained(), (1, 0));
}

fn assign(
    machine: &mut AssignedConsumerMachine,
    partitions: Vec<AssignedPartition>,
) -> kafka_client_core::AssignedConsumerTransition {
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions,
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("assign: {error}"))
}

fn entry(index: u32, start: StartPosition) -> AssignedPartition {
    AssignedPartition::new(partition(index), start)
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
