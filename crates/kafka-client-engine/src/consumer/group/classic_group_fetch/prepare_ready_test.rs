//! `FetchReady` FIFO, catalog, and attempt-boundary scenarios.

use kafka_client_core::{AssignedConsumerEffect, Deadline, Moment, TopicId};

use crate::{
    clock::MonotonicClock,
    protocol::fetch::{FetchDecodeLimits, FetchIsolation},
};

use super::{
    super::session_catalog::GroupSessionCatalogError,
    ClassicGroupFetchCapturedFailure, ClassicGroupFetchFront, ClassicGroupFetchOwner,
    ClassicGroupFetchOwnerFaultKind,
    owner::FIRST_GROUP_FETCH_OUTPUT_BYTES,
    test_support::{
        ATTEMPT_TIMEOUT_TICKS, assert_attempt_deadline, catalog, committed, completed_ready,
        position_fence,
    },
};

#[test]
fn exact_front_fetch_ready_captures_now_and_prepares_catalog_facts_fifo() {
    let catalog = catalog(&["orders", "payments"]);
    let fence = position_fence(7);
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
    let first_effect = owner
        .front_effect_for_test()
        .unwrap_or_else(|| panic!("first FetchReady"));
    let clock = MonotonicClock::new();
    let before = clock
        .now()
        .unwrap_or_else(|error| panic!("clock before preparation: {error}"));

    assert_eq!(
        owner.interpret_front_effect(&catalog, &clock),
        ClassicGroupFetchFront::Interpreted
    );

    let after = clock
        .now()
        .unwrap_or_else(|error| panic!("clock after preparation: {error}"));
    let first = owner
        .pop_prepared_for_test()
        .unwrap_or_else(|| panic!("first prepared Fetch"));
    assert_attempt_deadline(first.deadline(), before, after);
    assert_ne!(
        first.deadline(),
        Deadline::from_tick(41 + ATTEMPT_TIMEOUT_TICKS)
    );
    let (request, output_bytes) = first.into_parts_for_test();
    let AssignedConsumerEffect::FetchReady {
        fence: expected_fence,
        next_offset,
    } = first_effect
    else {
        panic!("first activation effect must be FetchReady");
    };
    assert_eq!(request.fence(), expected_fence);
    assert_eq!(request.next_offset(), next_offset);
    assert_eq!(request.topic(), "orders");
    assert_eq!(request.isolation(), Some(FetchIsolation::ReadUncommitted));
    assert_eq!(request.decode_limits(), FetchDecodeLimits::default());
    assert_eq!(output_bytes, FIRST_GROUP_FETCH_OUTPUT_BYTES);

    let second_effect = owner
        .front_effect_for_test()
        .unwrap_or_else(|| panic!("second FetchReady"));
    assert_eq!(
        owner.interpret_front_effect(&catalog, &clock),
        ClassicGroupFetchFront::Interpreted
    );
    let second = owner
        .pop_prepared_for_test()
        .unwrap_or_else(|| panic!("second prepared Fetch"));
    let (request, _) = second.into_parts_for_test();
    let AssignedConsumerEffect::FetchReady {
        fence: expected_fence,
        ..
    } = second_effect
    else {
        panic!("second activation effect must be FetchReady");
    };
    assert_eq!(request.fence(), expected_fence);
    assert_eq!(request.topic(), "payments");
    assert_eq!(owner.effect_count_for_test(), 0);
    assert_eq!(owner.pending_count_for_test(), 0);
}

#[test]
fn catalog_failure_retains_exact_effect_and_original_captured_attempt() {
    let fence = position_fence(7);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(2, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    let exact_effect = owner
        .front_effect_for_test()
        .unwrap_or_else(|| panic!("FetchReady retained"));
    let clock = MonotonicClock::new();
    let before = clock
        .now()
        .unwrap_or_else(|error| panic!("clock before preparation: {error}"));

    assert_eq!(
        owner.interpret_front_effect(&catalog(&["orders"]), &clock),
        ClassicGroupFetchFront::Idle
    );

    let after = clock
        .now()
        .unwrap_or_else(|error| panic!("clock after preparation: {error}"));
    assert_eq!(owner.front_effect_for_test(), Some(exact_effect));
    assert_eq!(owner.pending_count_for_test(), 0);
    let fault = owner
        .fault()
        .unwrap_or_else(|| panic!("captured-attempt fault retained"));
    assert_eq!(
        fault.kind(),
        ClassicGroupFetchOwnerFaultKind::Captured(ClassicGroupFetchCapturedFailure::Catalog(
            GroupSessionCatalogError::UnknownTopic(TopicId::from_raw(2))
        ))
    );
    assert_eq!(fault.effect(), Some(exact_effect));
    let attempt = fault
        .captured_attempt()
        .unwrap_or_else(|| panic!("attempt deadline retained"));
    let original_deadline = attempt.operation().core();
    assert_attempt_deadline(original_deadline, before, after);
    assert_eq!(
        owner.interpret_front_effect(&catalog(&["orders"]), &clock),
        ClassicGroupFetchFront::Idle
    );
    assert_eq!(
        owner
            .fault()
            .and_then(|retained| retained.captured_attempt())
            .map(|retained| retained.operation().core()),
        Some(original_deadline)
    );
    assert_eq!(owner.front_effect_for_test(), Some(exact_effect));
}
