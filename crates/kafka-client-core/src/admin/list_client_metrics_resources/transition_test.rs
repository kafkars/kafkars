//! Submission, normalization, bounds, failure, and terminal scenarios.

#![expect(
    clippy::needless_pass_by_value,
    reason = "test helpers preserve exact effect ownership"
)]

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCE_NAME_BYTES,
    LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCES, LIST_CLIENT_METRICS_RESOURCES_MAX_RETAINED_BYTES,
    ListClientMetricsResourcesBrokerError, ListClientMetricsResourcesEffect,
    ListClientMetricsResourcesFailureKind, ListClientMetricsResourcesInput,
    ListClientMetricsResourcesMachine, ListClientMetricsResourcesMachineError,
    ListClientMetricsResourcesState, ListClientMetricsResourcesTerminal,
};

#[test]
fn one_empty_request_reuses_the_original_public_deadline() {
    let mut machine = machine();
    let effect = effect(
        &mut machine,
        ListClientMetricsResourcesInput::Start {
            now: Moment::from_tick(2),
        },
    );
    assert!(matches!(
        effect,
        ListClientMetricsResourcesEffect::Submit {
            operation_id,
            deadline,
        } if operation_id == OperationId::from_raw(41)
            && deadline == Deadline::from_tick(900)
    ));
    assert_eq!(
        machine.state(),
        ListClientMetricsResourcesState::AwaitingDriver
    );
    accept(&mut machine);
    assert_eq!(machine.state(), ListClientMetricsResourcesState::Submitted);
}

#[test]
fn successful_response_is_canonicalized_in_strict_utf8_byte_order() {
    let mut machine = submitted_machine();
    let terminal = effect(
        &mut machine,
        ListClientMetricsResourcesInput::BrokerResponded {
            throttle_time_ms: 17,
            resource_names: vec!["\u{00e9}".to_owned(), "zeta".to_owned(), "alpha".to_owned()],
        },
    );
    let ListClientMetricsResourcesEffect::Complete {
        operation_id,
        terminal: ListClientMetricsResourcesTerminal::Listed(listing),
    } = terminal
    else {
        panic!("expected listed terminal");
    };
    assert_eq!(operation_id, OperationId::from_raw(41));
    assert_eq!(listing.throttle_time_ms(), 17);
    assert_eq!(listing.resource_names(), ["alpha", "zeta", "\u{00e9}"]);
    assert_eq!(machine.state(), ListClientMetricsResourcesState::Completed);
    assert_eq!(
        machine.apply(ListClientMetricsResourcesInput::InvalidResponse),
        Err(ListClientMetricsResourcesMachineError::AlreadyCompleted)
    );
}

#[test]
fn empty_duplicate_and_bounded_name_sets_fail_deterministically() {
    let mut empty = submitted_machine();
    assert_failure(
        effect(
            &mut empty,
            ListClientMetricsResourcesInput::BrokerResponded {
                throttle_time_ms: 0,
                resource_names: vec![String::new()],
            },
        ),
        ListClientMetricsResourcesFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );

    let mut duplicate = submitted_machine();
    assert_failure(
        effect(
            &mut duplicate,
            ListClientMetricsResourcesInput::BrokerResponded {
                throttle_time_ms: 0,
                resource_names: vec!["same".to_owned(), "same".to_owned()],
            },
        ),
        ListClientMetricsResourcesFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );

    let mut too_many = submitted_machine();
    assert_failure(
        effect(
            &mut too_many,
            ListClientMetricsResourcesInput::BrokerResponded {
                throttle_time_ms: 0,
                resource_names: (0..=LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCES)
                    .map(|index| index.to_string())
                    .collect(),
            },
        ),
        ListClientMetricsResourcesFailureKind::ResponseTooLarge,
        DeliveryStatus::PossiblySent,
    );

    let mut too_long = submitted_machine();
    assert_failure(
        effect(
            &mut too_long,
            ListClientMetricsResourcesInput::BrokerResponded {
                throttle_time_ms: 0,
                resource_names: vec![
                    "x".repeat(LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCE_NAME_BYTES + 1),
                ],
            },
        ),
        ListClientMetricsResourcesFailureKind::ResponseTooLarge,
        DeliveryStatus::PossiblySent,
    );

    let mut retained_too_large = submitted_machine();
    let resource_count = LIST_CLIENT_METRICS_RESOURCES_MAX_RETAINED_BYTES
        / LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCE_NAME_BYTES
        + 1;
    assert_failure(
        effect(
            &mut retained_too_large,
            ListClientMetricsResourcesInput::BrokerResponded {
                throttle_time_ms: 0,
                resource_names: (0..resource_count)
                    .map(|index| {
                        format!(
                            "{index:04}{}",
                            "x".repeat(LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCE_NAME_BYTES - 4)
                        )
                    })
                    .collect(),
            },
        ),
        ListClientMetricsResourcesFailureKind::ResponseTooLarge,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn exact_broker_rejection_is_a_distinct_terminal() {
    let mut machine = submitted_machine();
    let error = ListClientMetricsResourcesBrokerError::new(
        11,
        NonZeroI16::new(-17).unwrap_or_else(|| panic!("nonzero code")),
    );
    let terminal = effect(
        &mut machine,
        ListClientMetricsResourcesInput::BrokerRejected { error },
    );
    assert!(matches!(
        terminal,
        ListClientMetricsResourcesEffect::Complete {
            operation_id,
            terminal: ListClientMetricsResourcesTerminal::BrokerRejected(actual),
        } if operation_id == OperationId::from_raw(41) && actual == error
    ));
}

#[test]
fn deadline_and_mechanism_failures_preserve_delivery_certainty() {
    let mut expired = machine();
    assert_failure(
        effect(
            &mut expired,
            ListClientMetricsResourcesInput::Start {
                now: Moment::from_tick(900),
            },
        ),
        ListClientMetricsResourcesFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine();
    let _ = effect(
        &mut rejected,
        ListClientMetricsResourcesInput::Start {
            now: Moment::from_tick(1),
        },
    );
    assert_failure(
        effect(
            &mut rejected,
            ListClientMetricsResourcesInput::DriverRejected,
        ),
        ListClientMetricsResourcesFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );

    let mut transport = submitted_machine();
    assert_failure(
        effect(
            &mut transport,
            ListClientMetricsResourcesInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
        ),
        ListClientMetricsResourcesFailureKind::Transport,
        DeliveryStatus::NotSent,
    );
}

fn machine() -> ListClientMetricsResourcesMachine {
    ListClientMetricsResourcesMachine::new(OperationId::from_raw(41), Deadline::from_tick(900))
}

fn submitted_machine() -> ListClientMetricsResourcesMachine {
    let mut machine = machine();
    let _ = effect(
        &mut machine,
        ListClientMetricsResourcesInput::Start {
            now: Moment::from_tick(1),
        },
    );
    accept(&mut machine);
    machine
}

fn accept(machine: &mut ListClientMetricsResourcesMachine) {
    let transition = machine
        .apply(ListClientMetricsResourcesInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    assert!(transition.into_effect().is_none());
}

fn effect(
    machine: &mut ListClientMetricsResourcesMachine,
    input: ListClientMetricsResourcesInput,
) -> ListClientMetricsResourcesEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect"))
}

fn assert_failure(
    effect: ListClientMetricsResourcesEffect,
    expected_kind: ListClientMetricsResourcesFailureKind,
    expected_delivery: DeliveryStatus,
) {
    let ListClientMetricsResourcesEffect::Complete {
        terminal: ListClientMetricsResourcesTerminal::Failed(failure),
        ..
    } = effect
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(failure.delivery(), expected_delivery);
}
