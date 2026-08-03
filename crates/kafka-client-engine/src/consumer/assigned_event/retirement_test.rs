//! Assignment-retirement coverage for already-retained terminal events.

use std::sync::Arc;

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerMachine, FetchFailure, StartPosition,
};

use super::{
    AssignedConsumerEvent,
    test_support::{assign_reserved, entry, event_store, offset, retain},
};

#[test]
fn revoke_preserves_unobserved_terminal_events_for_the_application() {
    let mut machine = AssignedConsumerMachine::new();
    let mut store = event_store(2);
    let assigned = assign_reserved(
        &mut store,
        &mut machine,
        vec![
            entry(1, 0, StartPosition::Offset(offset(4))),
            entry(1, 1, StartPosition::Offset(offset(8))),
        ],
    );
    let AssignedConsumerEffect::FetchReady { fence: first, .. } = assigned.effects()[0] else {
        panic!("first Fetch claim");
    };
    let AssignedConsumerEffect::FetchReady { fence: second, .. } = assigned.effects()[1] else {
        panic!("second Fetch claim");
    };
    retain(
        &mut store,
        "orders",
        AssignedConsumerEffect::FetchFailed {
            fence: first,
            failure: FetchFailure::Transport,
        },
    );
    retain(
        &mut store,
        "orders",
        AssignedConsumerEffect::FetchFailed {
            fence: second,
            failure: FetchFailure::Transport,
        },
    );
    assert_eq!(store.retained(), (0, 2));

    store
        .observe_effect(AssignedConsumerEffect::Revoke {
            assignment_epoch: first.position().assignment_epoch(),
            partition: first.position().partition(),
        })
        .unwrap_or_else(|error| panic!("retire first terminal: {error:?}"));

    assert_eq!(store.retained(), (0, 2));
    assert_eq!(
        store.take_event(),
        Some(AssignedConsumerEvent::FetchFailed {
            topic: Arc::from("orders"),
            fence: first,
            failure: FetchFailure::Transport,
        })
    );
    assert_eq!(
        store.take_event(),
        Some(AssignedConsumerEvent::FetchFailed {
            topic: Arc::from("orders"),
            fence: second,
            failure: FetchFailure::Transport,
        })
    );
}
