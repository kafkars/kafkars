//! Incremental event-claim preservation across direct-assignment deltas.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, Deadline, FetchFailure,
    Moment, StartPosition,
};

use super::{
    AssignedConsumerEventStore,
    test_support::{assign_reserved, entry, event_store, offset, partition, retain},
};

#[test]
fn incremental_claims_preserve_unrelated_claims_and_ready_events() {
    let mut machine = AssignedConsumerMachine::new();
    let mut store = event_store(3);
    let assigned = assign_reserved(
        &mut store,
        &mut machine,
        vec![
            entry(1, 0, StartPosition::Offset(offset(1))),
            entry(1, 1, StartPosition::Offset(offset(2))),
        ],
    );
    let survivor = match assigned.effects()[0] {
        AssignedConsumerEffect::FetchReady { fence, .. } => fence,
        effect => panic!("survivor Fetch, got {effect:?}"),
    };
    retain(
        &mut store,
        "orders",
        AssignedConsumerEffect::FetchFailed {
            fence: survivor,
            failure: FetchFailure::Transport,
        },
    );
    assert_eq!(store.retained(), (1, 1));

    add_partition(&mut store, &mut machine);
    assert_eq!(store.retained(), (2, 1));

    let removal = store.prepare_removal(1);
    let transition = machine
        .apply(AssignedConsumerInput::RemoveAssignments {
            partitions: vec![partition(1, 1)],
        })
        .unwrap_or_else(|error| panic!("remove partition: {error}"));
    removal
        .commit_event_claims(transition.effects())
        .unwrap_or_else(|error| panic!("commit removal: {error:?}"));
    assert_eq!(store.retained(), (2, 1));
    store
        .observe_effect(transition.effects()[0])
        .unwrap_or_else(|error| panic!("observe removal: {error:?}"));
    assert_eq!(store.retained(), (1, 1));
    assert_eq!(store.claims[0].partition(), partition(1, 2));
}

fn add_partition(store: &mut AssignedConsumerEventStore, machine: &mut AssignedConsumerMachine) {
    let added = [entry(1, 2, StartPosition::Offset(offset(3)))];
    let claims = store
        .prepare_addition(&added)
        .unwrap_or_else(|error| panic!("prepare addition: {error:?}"));
    let transition = machine
        .apply(AssignedConsumerInput::AddAssignments {
            partitions: added.to_vec(),
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("add partition: {error}"));
    claims
        .commit_event_claims(transition.effects())
        .unwrap_or_else(|error| panic!("commit addition: {error:?}"));
}
