//! Deadline, canonical ordering, malformed-response, and terminal scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ClientQuotaMatch, DescribeClientQuotaEntity, DescribeClientQuotaEntityComponent,
    DescribeClientQuotaFilterComponent, DescribeClientQuotaValue, DescribeClientQuotasBatch,
    DescribeClientQuotasBrokerError, DescribeClientQuotasEffect, DescribeClientQuotasFailure,
    DescribeClientQuotasFailureKind, DescribeClientQuotasInput, DescribeClientQuotasMachine,
    DescribeClientQuotasMachineError, DescribeClientQuotasPlan, DescribeClientQuotasTerminal,
};

#[test]
fn one_submission_reuses_deadline_and_canonicalizes_every_result_level() {
    let mut machine = machine();
    let submit = effect(
        &mut machine,
        DescribeClientQuotasInput::Start {
            now: Moment::from_tick(1),
        },
    );
    let DescribeClientQuotasEffect::Submit {
        operation_id,
        deadline,
        plan,
    } = submit
    else {
        panic!("submit expected");
    };
    assert_eq!(operation_id, OperationId::from_raw(48));
    assert_eq!(deadline, Deadline::from_tick(100));
    assert_eq!(plan.components()[0].entity_type(), "user");

    assert!(
        machine
            .apply(DescribeClientQuotasInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accept: {error}"))
            .into_effect()
            .is_none()
    );
    let terminal = effect(
        &mut machine,
        DescribeClientQuotasInput::BrokerResponded {
            batch: DescribeClientQuotasBatch::new(
                11,
                vec![
                    entity(
                        vec![("user", Some("zed"))],
                        vec![("request_percentage", 75.0)],
                    ),
                    entity(
                        vec![("user", Some("alice")), ("client-id", None)],
                        vec![
                            ("producer_byte_rate", 2048.0),
                            ("consumer_byte_rate", 1024.0),
                        ],
                    ),
                    entity(vec![("user", None)], vec![("request_percentage", 50.0)]),
                ],
            ),
        },
    );
    let DescribeClientQuotasEffect::Complete {
        terminal: DescribeClientQuotasTerminal::Described(batch),
        ..
    } = terminal
    else {
        panic!("described terminal expected");
    };
    assert_eq!(batch.throttle_time_ms(), 11);
    assert_eq!(
        batch.entities()[0].components()[0].entity_type(),
        "client-id"
    );
    assert_eq!(batch.entities()[0].components()[1].entity_type(), "user");
    assert_eq!(
        batch.entities()[0]
            .values()
            .iter()
            .map(DescribeClientQuotaValue::key)
            .collect::<Vec<_>>(),
        vec!["consumer_byte_rate", "producer_byte_rate"]
    );
    assert_eq!(batch.entities()[1].components()[0].entity_name(), None);
    assert_eq!(
        batch.entities()[2].components()[0].entity_name(),
        Some("zed")
    );
    assert_eq!(
        machine.apply(DescribeClientQuotasInput::InvalidResponse),
        Err(DescribeClientQuotasMachineError::AlreadyCompleted)
    );
}

#[test]
fn duplicate_or_malformed_entity_facts_fail_without_partial_success() {
    let malformed_batches = [
        DescribeClientQuotasBatch::new(
            0,
            vec![entity(
                vec![("user", Some("alice")), ("user", None)],
                vec![("producer_byte_rate", 1.0)],
            )],
        ),
        DescribeClientQuotasBatch::new(
            0,
            vec![entity(
                vec![("user", Some("alice"))],
                vec![("producer_byte_rate", 1.0), ("producer_byte_rate", 2.0)],
            )],
        ),
        DescribeClientQuotasBatch::new(
            0,
            vec![
                entity(
                    vec![("user", Some("alice"))],
                    vec![("producer_byte_rate", 1.0)],
                ),
                entity(
                    vec![("user", Some("alice"))],
                    vec![("consumer_byte_rate", 2.0)],
                ),
            ],
        ),
        DescribeClientQuotasBatch::new(
            0,
            vec![entity(
                vec![("user", Some("alice"))],
                vec![("producer_byte_rate", f64::NAN)],
            )],
        ),
        DescribeClientQuotasBatch::new(
            0,
            vec![DescribeClientQuotaEntity::new(
                Vec::new(),
                vec![DescribeClientQuotaValue::new(
                    "producer_byte_rate".to_owned(),
                    1.0,
                )],
            )],
        ),
    ];

    for batch in malformed_batches {
        let terminal = effect(
            &mut submitted_machine(),
            DescribeClientQuotasInput::BrokerResponded { batch },
        );
        let failure = failure(terminal);
        assert_eq!(
            failure.kind(),
            &DescribeClientQuotasFailureKind::InvalidResponse
        );
        assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    }
}

#[test]
fn broker_and_transport_failures_preserve_exact_delivery_facts() {
    let broker_terminal = effect(
        &mut submitted_machine(),
        DescribeClientQuotasInput::BrokerRejected {
            error: DescribeClientQuotasBrokerError::new(
                NonZeroI16::new(-29).unwrap_or_else(|| panic!("nonzero")),
                Some("denied".to_owned()),
                false,
            ),
        },
    );
    let broker_failure = failure(broker_terminal);
    let DescribeClientQuotasFailureKind::Broker(error) = broker_failure.kind() else {
        panic!("broker failure expected");
    };
    assert_eq!(error.code(), -29);
    assert_eq!(error.message(), Some("denied"));
    assert_eq!(broker_failure.delivery(), DeliveryStatus::PossiblySent);

    let transport_terminal = effect(
        &mut submitted_machine(),
        DescribeClientQuotasInput::TransportFailed {
            delivery: DeliveryStatus::NotSent,
        },
    );
    let transport_failure = failure(transport_terminal);
    assert_eq!(
        transport_failure.kind(),
        &DescribeClientQuotasFailureKind::Transport
    );
    assert_eq!(transport_failure.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn elapsed_original_deadline_is_definitely_unsent() {
    let terminal = effect(
        &mut machine(),
        DescribeClientQuotasInput::Start {
            now: Moment::from_tick(100),
        },
    );
    let failure = failure(terminal);
    assert_eq!(
        failure.kind(),
        &DescribeClientQuotasFailureKind::DeadlineElapsed
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}

fn machine() -> DescribeClientQuotasMachine {
    DescribeClientQuotasMachine::new(
        OperationId::from_raw(48),
        Deadline::from_tick(100),
        DescribeClientQuotasPlan::new(
            vec![DescribeClientQuotaFilterComponent::new(
                "user".to_owned(),
                ClientQuotaMatch::AnySpecified,
            )],
            false,
        )
        .unwrap_or_else(|error| panic!("valid filter: {error}")),
    )
}

fn submitted_machine() -> DescribeClientQuotasMachine {
    let mut machine = machine();
    effect(
        &mut machine,
        DescribeClientQuotasInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
        .apply(DescribeClientQuotasInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    machine
}

fn entity(
    components: Vec<(&str, Option<&str>)>,
    values: Vec<(&str, f64)>,
) -> DescribeClientQuotaEntity {
    DescribeClientQuotaEntity::new(
        components
            .into_iter()
            .map(|(entity_type, entity_name)| {
                DescribeClientQuotaEntityComponent::new(
                    entity_type.to_owned(),
                    entity_name.map(str::to_owned),
                )
            })
            .collect(),
        values
            .into_iter()
            .map(|(key, value)| DescribeClientQuotaValue::new(key.to_owned(), value))
            .collect(),
    )
}

fn effect(
    machine: &mut DescribeClientQuotasMachine,
    input: DescribeClientQuotasInput,
) -> DescribeClientQuotasEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect expected"))
}

fn failure(effect: DescribeClientQuotasEffect) -> DescribeClientQuotasFailure {
    let DescribeClientQuotasEffect::Complete {
        terminal: DescribeClientQuotasTerminal::Failed(failure),
        ..
    } = effect
    else {
        panic!("failed terminal expected");
    };
    failure
}
