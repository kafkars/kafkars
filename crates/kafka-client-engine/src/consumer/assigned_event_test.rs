//! FIFO terminal transfer, bounded backpressure, and stale-fence scenarios.

use std::{num::NonZeroI16, sync::Arc};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerMachine, FetchFailure, FetchThrottleFailure,
    PositionResolutionFailure, StartPosition,
};

use super::assigned_event::test_support::{
    assign, assign_reserved, entry, event_store, offset, partition, retain,
};
use super::assigned_event::{AssignedConsumerEvent, AssignedConsumerEventStoreError};

#[test]
fn terminal_effects_transfer_exact_claims_into_fifo_events() {
    let mut machine = AssignedConsumerMachine::new();
    let mut store = event_store(3);
    let prepared = store
        .prepare_replacement(3)
        .unwrap_or_else(|error| panic!("reserve assignment events: {error:?}"));
    let assigned = assign(
        &mut machine,
        vec![
            entry(1, 0, StartPosition::Beginning),
            entry(1, 1, StartPosition::Offset(offset(4))),
            entry(1, 2, StartPosition::Offset(offset(8))),
        ],
    );
    prepared
        .commit_event_claims(assigned.effects())
        .unwrap_or_else(|error| panic!("commit assignment events: {error:?}"));
    let AssignedConsumerEffect::ResolvePosition {
        fence: position, ..
    } = assigned.effects()[0]
    else {
        panic!("position claim");
    };
    let AssignedConsumerEffect::FetchReady {
        fence: throttle, ..
    } = assigned.effects()[1]
    else {
        panic!("throttle claim");
    };
    let AssignedConsumerEffect::FetchReady { fence: fetch, .. } = assigned.effects()[2] else {
        panic!("fetch claim");
    };
    let broker = NonZeroI16::new(-7).unwrap_or_else(|| panic!("nonzero broker error"));
    retain(
        &mut store,
        "positions",
        AssignedConsumerEffect::PositionResolutionFailed {
            fence: position,
            failure: PositionResolutionFailure::DeadlineElapsed,
        },
    );
    retain(
        &mut store,
        "throttles",
        AssignedConsumerEffect::FetchThrottleFailed {
            fence: throttle,
            failure: FetchThrottleFailure::DeadlineOverflow,
        },
    );
    retain(
        &mut store,
        "fetches",
        AssignedConsumerEffect::FetchFailed {
            fence: fetch,
            failure: FetchFailure::Broker(broker),
        },
    );

    assert_eq!(store.retained(), (0, 3));
    assert_eq!(
        store.take_event(),
        Some(AssignedConsumerEvent::PositionResolutionFailed {
            topic: Arc::from("positions"),
            fence: position,
            failure: PositionResolutionFailure::DeadlineElapsed,
        })
    );
    assert_eq!(
        store.take_event(),
        Some(AssignedConsumerEvent::FetchThrottleFailed {
            topic: Arc::from("throttles"),
            fence: throttle,
            failure: FetchThrottleFailure::DeadlineOverflow,
        })
    );
    assert_eq!(
        store.take_event(),
        Some(AssignedConsumerEvent::FetchFailed {
            topic: Arc::from("fetches"),
            fence: fetch,
            failure: FetchFailure::Broker(broker),
        })
    );
}

#[test]
fn stale_terminal_fence_cannot_consume_active_claim() {
    let mut machine = AssignedConsumerMachine::new();
    let mut store = event_store(1);
    let first = assign_reserved(
        &mut store,
        &mut machine,
        vec![entry(1, 0, StartPosition::Beginning)],
    );
    let AssignedConsumerEffect::ResolvePosition { fence: stale, .. } = first.effects()[0] else {
        panic!("first position");
    };
    let replacement = assign_reserved(
        &mut store,
        &mut machine,
        vec![entry(1, 0, StartPosition::Beginning)],
    );
    let revoke = replacement.effects()[0];
    let AssignedConsumerEffect::ResolvePosition { fence: current, .. } = replacement.effects()[1]
    else {
        panic!("replacement position");
    };
    store
        .observe_effect(revoke)
        .unwrap_or_else(|error| panic!("stale revoke observation: {error:?}"));
    let error = store
        .retain_terminal(
            Arc::from("orders"),
            AssignedConsumerEffect::PositionResolutionFailed {
                fence: stale,
                failure: PositionResolutionFailure::DeadlineElapsed,
            },
        )
        .err()
        .unwrap_or_else(|| panic!("stale terminal must reject"));
    assert_eq!(error.0, AssignedConsumerEventStoreError::ClaimMismatch);
    assert_eq!(store.retained(), (1, 0));
    retain(
        &mut store,
        "orders",
        AssignedConsumerEffect::PositionResolutionFailed {
            fence: current,
            failure: PositionResolutionFailure::DeadlineElapsed,
        },
    );
}

#[test]
fn ready_events_backpressure_new_claims_until_observed() {
    let mut machine = AssignedConsumerMachine::new();
    let mut store = event_store(1);
    let assigned = assign_reserved(
        &mut store,
        &mut machine,
        vec![entry(1, 0, StartPosition::Beginning)],
    );
    let AssignedConsumerEffect::ResolvePosition { fence, .. } = assigned.effects()[0] else {
        panic!("position claim");
    };
    retain(
        &mut store,
        "orders",
        AssignedConsumerEffect::PositionResolutionFailed {
            fence,
            failure: PositionResolutionFailure::DeadlineElapsed,
        },
    );

    assert!(matches!(
        store.prepare_replacement(1),
        Err(AssignedConsumerEventStoreError::Capacity)
    ));
    assert!(matches!(
        store.prepare_partition(partition(2, 0)),
        Err(AssignedConsumerEventStoreError::Capacity)
    ));
    let _event = store
        .take_event()
        .unwrap_or_else(|| panic!("retained terminal event"));
    store
        .prepare_replacement(1)
        .unwrap_or_else(|error| panic!("released event capacity: {error:?}"))
        .rollback_event_claims();
}

#[test]
fn revoke_removes_only_matching_assignment_claim() {
    let mut machine = AssignedConsumerMachine::new();
    let mut store = event_store(1);
    let _first = assign_reserved(
        &mut store,
        &mut machine,
        vec![entry(1, 0, StartPosition::Beginning)],
    );
    let replacement = assign_reserved(
        &mut store,
        &mut machine,
        vec![entry(1, 0, StartPosition::Beginning)],
    );
    let stale_revoke = replacement.effects()[0];
    store
        .observe_effect(stale_revoke)
        .unwrap_or_else(|error| panic!("stale revoke: {error:?}"));
    assert_eq!(store.retained(), (1, 0));
    let epoch = replacement
        .assignment_epoch()
        .unwrap_or_else(|| panic!("replacement epoch"));
    store
        .observe_effect(AssignedConsumerEffect::Revoke {
            assignment_epoch: epoch,
            partition: partition(1, 0),
        })
        .unwrap_or_else(|error| panic!("current revoke: {error:?}"));
    assert_eq!(store.retained(), (0, 0));
}

#[test]
fn post_driver_recovery_counts_and_releases_all_event_ownership() {
    let mut machine = AssignedConsumerMachine::new();
    let mut store = event_store(2);
    let assigned = assign_reserved(
        &mut store,
        &mut machine,
        vec![
            entry(1, 0, StartPosition::Beginning),
            entry(1, 1, StartPosition::Beginning),
        ],
    );
    let AssignedConsumerEffect::ResolvePosition { fence, .. } = assigned.effects()[0] else {
        panic!("position claim");
    };
    retain(
        &mut store,
        "orders",
        AssignedConsumerEffect::PositionResolutionFailed {
            fence,
            failure: PositionResolutionFailure::DeadlineElapsed,
        },
    );

    let recovery = store.recover_after_driver_shutdown();
    assert_eq!((recovery.claimed(), recovery.ready()), (1, 1));
    assert_eq!(store.retained(), (0, 0));
}
