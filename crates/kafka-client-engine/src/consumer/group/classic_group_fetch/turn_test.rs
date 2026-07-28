//! Stage order, deadline settlement, and lossless pressure scenarios for group Fetch turns.

use std::time::Duration;

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, FetchFailure, FetchRecords, Moment,
    NextFetchOffset,
};

use crate::{
    clock::MonotonicClock,
    consumer::{
        assigned_owner_test::{driver, shutdown},
        fetch_execution::{
            DirectFetchExecutor, FetchExecutionError, FetchTerminalFixture,
            install_terminal_for_test,
        },
    },
};

use super::{
    ClassicGroupFetchOwner, ClassicGroupFetchOwnerFaultKind,
    owner::FIRST_GROUP_FETCH_DELIVERY_BYTES,
    test_support::{catalog, committed, completed_ready, position_fence},
};

#[test]
fn effects_end_the_turn_and_terminal_poll_precedes_the_next_submission() {
    let catalog = catalog(&["orders", "payments"]);
    let fence = position_fence(7);
    let clock = MonotonicClock::new();
    let mut driver = driver();
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(
                fence,
                Moment::from_tick(41),
                0,
                vec![committed(1, 1, 17), committed(2, 4, 23)],
            ),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));

    let first = owner.turn(&catalog, &clock, &driver);
    assert!(first.effect_interpreted());
    assert!(!first.fetch_polled());
    assert!(!first.fetch_submitted());
    assert_eq!(owner.pending_count_for_test(), 1);
    assert_eq!(owner.effect_count_for_test(), 1);

    let second = owner.turn(&catalog, &clock, &driver);
    assert!(second.effect_interpreted());
    assert!(!second.fetch_polled());
    assert!(!second.fetch_submitted());
    assert_eq!(owner.pending_count_for_test(), 2);
    assert_eq!(owner.effect_count_for_test(), 0);

    let completed = owner
        .pending_fetches
        .pop_front()
        .unwrap_or_else(|| panic!("first prepared Fetch"));
    install_terminal_for_test(
        &mut owner.fetches,
        completed,
        FetchTerminalFixture::Success(None),
    );
    let retained_pending = owner.pending_fetches.front().map_or_else(
        || panic!("second prepared Fetch"),
        |prepared| (prepared.fence(), prepared.deadline()),
    );

    let settlement = owner.turn(&catalog, &clock, &driver);
    assert!(settlement.fetch_polled());
    assert!(!settlement.fetch_submitted());
    assert_eq!(
        owner
            .pending_fetches
            .front()
            .map(|prepared| (prepared.fence(), prepared.deadline())),
        Some(retained_pending)
    );
    assert!(matches!(
        owner.effects.front(),
        Some(AssignedConsumerEffect::FetchReady { .. })
    ));

    shutdown(&mut driver);
}

#[test]
fn elapsed_attempt_settles_before_driver_or_output_capacity() {
    let catalog = catalog(&["orders"]);
    let fence = position_fence(7);
    let clock = MonotonicClock::new();
    let mut driver = driver();
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner.fetch_attempt_timeout = Duration::ZERO;
    owner.fetches = DirectFetchExecutor::create_unbound(0, 0, 0);
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(1, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));

    let preparation = owner.turn(&catalog, &clock, &driver);
    assert!(preparation.effect_interpreted());
    assert_eq!(owner.pending_count_for_test(), 1);

    let settlement = owner.turn(&catalog, &clock, &driver);
    assert!(settlement.fetch_submitted());
    assert!(!settlement.blocked());
    assert_eq!(owner.pending_count_for_test(), 0);
    assert_eq!(owner.fetches.retained(), (0, 0, 0));
    assert!(matches!(
        owner.effects.front(),
        Some(AssignedConsumerEffect::FetchFailed {
            failure: FetchFailure::DeadlineElapsed,
            ..
        })
    ));
    assert!(owner.fault().is_none());

    shutdown(&mut driver);
}

#[test]
fn one_due_timer_input_precedes_poll_and_pending_submission() {
    let catalog = catalog(&["orders"]);
    let fence = position_fence(7);
    let clock = MonotonicClock::new();
    let mut driver = driver();
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(1, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    assert!(owner.turn(&catalog, &clock, &driver).effect_interpreted());
    let prepared = owner.pending_fetches.front().map_or_else(
        || panic!("prepared Fetch"),
        |prepared| (prepared.fence(), prepared.deadline()),
    );
    let transition = owner
        .machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: prepared.0,
            records: FetchRecords::NoApplicationRecords,
            next_offset: NextFetchOffset::try_from_raw(18).unwrap_or_else(|| panic!("next offset")),
            now: Moment::from_tick(0),
            throttle_ticks: 1,
        })
        .unwrap_or_else(|error| panic!("stage throttled Fetch: {error}"));
    for effect in transition.into_effects() {
        owner.effects.push_back(effect);
    }
    let arm = owner.turn(&catalog, &clock, &driver);
    assert!(arm.effect_interpreted());
    assert_eq!(owner.timer_count_for_test(), 1);

    let elapsed = owner.turn(&catalog, &clock, &driver);

    assert!(elapsed.timer_input_applied());
    assert!(!elapsed.fetch_polled());
    assert!(!elapsed.fetch_submitted());
    assert_eq!(
        owner
            .pending_fetches
            .front()
            .map(|pending| (pending.fence(), pending.deadline())),
        Some(prepared)
    );
    assert!(matches!(
        owner.effects.front(),
        Some(AssignedConsumerEffect::FetchReady { .. })
    ));

    shutdown(&mut driver);
}

#[test]
fn call_backpressure_restores_the_exact_prepared_owner_for_retry() {
    let catalog = catalog(&["orders"]);
    let fence = position_fence(7);
    let clock = MonotonicClock::new();
    let mut driver = driver();
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner.fetches = DirectFetchExecutor::create_unbound(0, 1, FIRST_GROUP_FETCH_DELIVERY_BYTES);
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(1, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    assert!(owner.turn(&catalog, &clock, &driver).effect_interpreted());
    let exact = owner.pending_fetches.front().map_or_else(
        || panic!("prepared Fetch"),
        |prepared| (prepared.fence(), prepared.deadline()),
    );

    let blocked = owner.turn(&catalog, &clock, &driver);

    assert!(blocked.blocked());
    assert!(!blocked.progressed());
    assert_eq!(
        owner
            .pending_fetches
            .front()
            .map(|prepared| (prepared.fence(), prepared.deadline())),
        Some(exact)
    );
    assert_eq!(owner.fetches.retained(), (0, 0, 0));
    assert!(owner.fault().is_none());

    shutdown(&mut driver);
}

#[test]
fn unavailable_executor_restores_prepared_ownership_and_freezes_the_owner() {
    let catalog = catalog(&["orders"]);
    let fence = position_fence(7);
    let clock = MonotonicClock::new();
    let mut driver = driver();
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(1, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    assert!(owner.turn(&catalog, &clock, &driver).effect_interpreted());
    let exact = owner.pending_fetches.front().map_or_else(
        || panic!("prepared Fetch"),
        |prepared| (prepared.fence(), prepared.deadline()),
    );
    owner.fetches.install_fault_for_test();

    let unavailable = owner.turn(&catalog, &clock, &driver);

    assert!(unavailable.fault_retained());
    assert_eq!(
        owner
            .pending_fetches
            .front()
            .map(|prepared| (prepared.fence(), prepared.deadline())),
        Some(exact)
    );
    assert_eq!(
        owner
            .fault()
            .map(super::model::ClassicGroupFetchOwnerFault::kind),
        Some(ClassicGroupFetchOwnerFaultKind::Fetch(
            FetchExecutionError::Faulted
        ))
    );

    shutdown(&mut driver);
}
