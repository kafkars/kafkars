//! Submission, normalization, bounds, failure, and terminal scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ConfigResourceType, LIST_CONFIG_RESOURCES_MAX_RESOURCE_NAME_BYTES,
    LIST_CONFIG_RESOURCES_MAX_RESOURCES, ListConfigResourcesBrokerError, ListConfigResourcesEffect,
    ListConfigResourcesFailureKind, ListConfigResourcesInput, ListConfigResourcesMachine,
    ListConfigResourcesMachineError, ListConfigResourcesPlan, ListConfigResourcesState,
    ListConfigResourcesTerminal, ListedConfigResource,
};

#[test]
fn one_request_preserves_plan_order_and_original_public_deadline() {
    let mut machine = machine();
    let effect = effect(
        &mut machine,
        ListConfigResourcesInput::Start {
            now: Moment::from_tick(2),
        },
    );
    let ListConfigResourcesEffect::Submit {
        operation_id,
        deadline,
        plan,
    } = effect
    else {
        panic!("expected submit");
    };
    assert_eq!(operation_id, OperationId::from_raw(74));
    assert_eq!(deadline, Deadline::from_tick(900));
    assert_eq!(
        plan.resource_types(),
        [ConfigResourceType::GROUP, ConfigResourceType::TOPIC]
    );
    assert_eq!(machine.state(), ListConfigResourcesState::AwaitingDriver);
    accept(&mut machine);
    assert_eq!(machine.state(), ListConfigResourcesState::Submitted);
}

#[test]
fn successful_response_is_canonicalized_by_signed_type_then_name_bytes() {
    let mut machine = submitted_machine();
    let terminal = effect(
        &mut machine,
        ListConfigResourcesInput::BrokerResponded {
            throttle_time_ms: 17,
            resources: vec![
                resource(ConfigResourceType::GROUP, "workers"),
                resource(ConfigResourceType::TOPIC, "\u{00e9}"),
                resource(ConfigResourceType::TOPIC, "zeta"),
                resource(ConfigResourceType::TOPIC, "alpha"),
                resource(
                    ConfigResourceType::new(3)
                        .unwrap_or_else(|error| panic!("future type: {error}")),
                    "future",
                ),
            ],
        },
    );
    let ListConfigResourcesEffect::Complete {
        operation_id,
        terminal: ListConfigResourcesTerminal::Listed(listing),
    } = terminal
    else {
        panic!("expected listed terminal");
    };
    assert_eq!(operation_id, OperationId::from_raw(74));
    assert_eq!(listing.throttle_time_ms(), 17);
    let keys = listing
        .resources()
        .iter()
        .map(|entry| (entry.resource_type().code(), entry.resource_name()))
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        vec![
            (2, "alpha"),
            (2, "zeta"),
            (2, "\u{00e9}"),
            (3, "future"),
            (32, "workers"),
        ]
    );
    assert_eq!(machine.state(), ListConfigResourcesState::Completed);
    assert_eq!(
        machine.apply(ListConfigResourcesInput::InvalidResponse),
        Err(ListConfigResourcesMachineError::AlreadyCompleted)
    );
}

#[test]
fn duplicate_and_empty_resource_identities_are_invalid() {
    let mut duplicate = submitted_machine();
    assert_failure(
        effect(
            &mut duplicate,
            ListConfigResourcesInput::BrokerResponded {
                throttle_time_ms: 0,
                resources: vec![
                    resource(ConfigResourceType::TOPIC, "same"),
                    resource(ConfigResourceType::TOPIC, "same"),
                ],
            },
        ),
        ListConfigResourcesFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );

    let mut empty = submitted_machine();
    assert_failure(
        effect(
            &mut empty,
            ListConfigResourcesInput::BrokerResponded {
                throttle_time_ms: 0,
                resources: vec![resource(ConfigResourceType::TOPIC, "")],
            },
        ),
        ListConfigResourcesFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn count_name_and_aggregate_text_bounds_fail_as_response_too_large() {
    let mut too_many = submitted_machine();
    assert_failure(
        effect(
            &mut too_many,
            ListConfigResourcesInput::BrokerResponded {
                throttle_time_ms: 0,
                resources: (0..=LIST_CONFIG_RESOURCES_MAX_RESOURCES)
                    .map(|index| resource(ConfigResourceType::TOPIC, &index.to_string()))
                    .collect(),
            },
        ),
        ListConfigResourcesFailureKind::ResponseTooLarge,
        DeliveryStatus::PossiblySent,
    );

    let mut too_long = submitted_machine();
    assert_failure(
        effect(
            &mut too_long,
            ListConfigResourcesInput::BrokerResponded {
                throttle_time_ms: 0,
                resources: vec![resource(
                    ConfigResourceType::TOPIC,
                    &"x".repeat(LIST_CONFIG_RESOURCES_MAX_RESOURCE_NAME_BYTES + 1),
                )],
            },
        ),
        ListConfigResourcesFailureKind::ResponseTooLarge,
        DeliveryStatus::PossiblySent,
    );

    let mut aggregate = submitted_machine();
    assert_failure(
        effect(
            &mut aggregate,
            ListConfigResourcesInput::BrokerResponded {
                throttle_time_ms: 0,
                resources: (0..LIST_CONFIG_RESOURCES_MAX_RESOURCES)
                    .map(|index| {
                        resource(
                            ConfigResourceType::TOPIC,
                            &format!("{index:04}{}", "x".repeat(253)),
                        )
                    })
                    .collect(),
            },
        ),
        ListConfigResourcesFailureKind::ResponseTooLarge,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn exact_broker_rejection_and_mechanism_failures_preserve_certainty() {
    let mut rejected = submitted_machine();
    let error = ListConfigResourcesBrokerError::new(
        11,
        NonZeroI16::new(-17).unwrap_or_else(|| panic!("nonzero code")),
    );
    assert!(matches!(
        effect(
            &mut rejected,
            ListConfigResourcesInput::BrokerRejected { error },
        ),
        ListConfigResourcesEffect::Complete {
            terminal: ListConfigResourcesTerminal::BrokerRejected(actual),
            ..
        } if actual == error
    ));

    let mut expired = machine();
    assert_failure(
        effect(
            &mut expired,
            ListConfigResourcesInput::Start {
                now: Moment::from_tick(900),
            },
        ),
        ListConfigResourcesFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut transport = submitted_machine();
    assert_failure(
        effect(
            &mut transport,
            ListConfigResourcesInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        ),
        ListConfigResourcesFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
}

fn resource(resource_type: ConfigResourceType, name: &str) -> ListedConfigResource {
    ListedConfigResource::new(resource_type, name.to_owned())
}

fn machine() -> ListConfigResourcesMachine {
    let plan =
        ListConfigResourcesPlan::new(vec![ConfigResourceType::GROUP, ConfigResourceType::TOPIC])
            .unwrap_or_else(|error| panic!("valid plan: {error}"));
    ListConfigResourcesMachine::new(OperationId::from_raw(74), Deadline::from_tick(900), plan)
}

fn submitted_machine() -> ListConfigResourcesMachine {
    let mut machine = machine();
    let _ = effect(
        &mut machine,
        ListConfigResourcesInput::Start {
            now: Moment::from_tick(1),
        },
    );
    accept(&mut machine);
    machine
}

fn accept(machine: &mut ListConfigResourcesMachine) {
    let transition = machine
        .apply(ListConfigResourcesInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    assert!(transition.into_effect().is_none());
}

fn effect(
    machine: &mut ListConfigResourcesMachine,
    input: ListConfigResourcesInput,
) -> ListConfigResourcesEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("valid transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("expected effect"))
}

fn assert_failure(
    effect: ListConfigResourcesEffect,
    expected_kind: ListConfigResourcesFailureKind,
    expected_delivery: DeliveryStatus,
) {
    let ListConfigResourcesEffect::Complete {
        terminal: ListConfigResourcesTerminal::Failed(failure),
        ..
    } = effect
    else {
        panic!("expected failure terminal");
    };
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(failure.delivery(), expected_delivery);
}
