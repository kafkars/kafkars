//! Transition, delivery, and response-correlation scenarios for `AlterClientQuotas`.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ALTER_CLIENT_QUOTAS_DIAGNOSTIC_BYTES, AlterClientQuotaBrokerError, AlterClientQuotaEntity,
    AlterClientQuotaEntityComponent, AlterClientQuotaEntry, AlterClientQuotaOperation,
    AlterClientQuotaOutcome, AlterClientQuotaResult, AlterClientQuotasBatch,
    AlterClientQuotasEffect, AlterClientQuotasFailureKind, AlterClientQuotasInput,
    AlterClientQuotasMachine, AlterClientQuotasMachineError, AlterClientQuotasPlan,
    AlterClientQuotasState, AlterClientQuotasTerminal, AlterClientQuotasTransition,
};

#[test]
fn response_is_canonically_correlated_and_restored_to_caller_order() {
    let mut machine = submitted_machine();
    let code = NonZeroI16::new(-32_111).unwrap_or_else(|| panic!("code is nonzero"));
    let batch = AlterClientQuotasBatch::new(
        73,
        vec![
            AlterClientQuotaOutcome::failed(
                entity(vec![("ip", None)]),
                AlterClientQuotaBrokerError::new(code, Some("not allowed".to_owned()), false),
            ),
            AlterClientQuotaOutcome::altered(entity(vec![
                ("user", Some("alice")),
                ("client-id", Some("app")),
            ])),
        ],
    );
    let transition = machine
        .apply(AlterClientQuotasInput::BrokerResponded { batch })
        .unwrap_or_else(|error| panic!("correlated response should settle: {error}"));
    let Some(AlterClientQuotasEffect::Complete {
        terminal: AlterClientQuotasTerminal::Altered(batch),
        ..
    }) = transition.into_effect()
    else {
        panic!("valid response must complete");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(
        batch.outcomes()[0].entity().components()[0].entity_type(),
        "client-id"
    );
    assert_eq!(
        batch.outcomes()[1].entity().components()[0].entity_type(),
        "ip"
    );
    let AlterClientQuotaResult::Failed(error) = batch.outcomes()[1].result() else {
        panic!("ip result must retain exact broker failure");
    };
    assert_eq!(error.code(), -32_111);
    assert_eq!(machine.state(), AlterClientQuotasState::Completed);
    assert_eq!(
        machine.apply(AlterClientQuotasInput::InvalidResponse),
        Err(AlterClientQuotasMachineError::AlreadyCompleted)
    );
}

#[test]
fn missing_extra_duplicate_unexpected_and_malformed_entities_fail_once() {
    let malformed = [
        AlterClientQuotasBatch::new(0, vec![AlterClientQuotaOutcome::altered(ip_entity())]),
        AlterClientQuotasBatch::new(
            0,
            vec![
                AlterClientQuotaOutcome::altered(user_entity()),
                AlterClientQuotaOutcome::altered(ip_entity()),
                AlterClientQuotaOutcome::altered(entity(vec![("client-id", Some("other"))])),
            ],
        ),
        AlterClientQuotasBatch::new(
            0,
            vec![
                AlterClientQuotaOutcome::altered(user_entity()),
                AlterClientQuotaOutcome::altered(user_entity()),
            ],
        ),
        AlterClientQuotasBatch::new(
            0,
            vec![
                AlterClientQuotaOutcome::altered(user_entity()),
                AlterClientQuotaOutcome::altered(entity(vec![("client-id", Some("other"))])),
            ],
        ),
        AlterClientQuotasBatch::new(
            0,
            vec![
                AlterClientQuotaOutcome::altered(AlterClientQuotaEntity::new(Vec::new())),
                AlterClientQuotaOutcome::altered(ip_entity()),
            ],
        ),
        AlterClientQuotasBatch::new(
            0,
            vec![
                AlterClientQuotaOutcome::altered(entity(vec![
                    ("user", Some("alice")),
                    ("user", Some("bob")),
                ])),
                AlterClientQuotaOutcome::altered(ip_entity()),
            ],
        ),
    ];
    for batch in malformed {
        assert_invalid_response(batch);
    }
}

#[test]
fn oversized_diagnostic_is_rejected_even_when_entity_correlation_is_exact() {
    let code = NonZeroI16::new(1).unwrap_or_else(|| panic!("code is nonzero"));
    assert_invalid_response(AlterClientQuotasBatch::new(
        0,
        vec![
            AlterClientQuotaOutcome::failed(
                user_entity(),
                AlterClientQuotaBrokerError::new(
                    code,
                    Some("x".repeat(ALTER_CLIENT_QUOTAS_DIAGNOSTIC_BYTES + 1)),
                    false,
                ),
            ),
            AlterClientQuotaOutcome::altered(ip_entity()),
        ],
    ));
}

fn assert_invalid_response(batch: AlterClientQuotasBatch) {
    let mut machine = submitted_machine();
    let transition = machine
        .apply(AlterClientQuotasInput::BrokerResponded { batch })
        .unwrap_or_else(|error| panic!("malformed response should settle: {error}"));
    assert_failure(
        transition,
        AlterClientQuotasFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
    assert_eq!(
        machine.apply(AlterClientQuotasInput::InvalidResponse),
        Err(AlterClientQuotasMachineError::AlreadyCompleted)
    );
}

fn assert_failure(
    transition: AlterClientQuotasTransition,
    kind: AlterClientQuotasFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(AlterClientQuotasEffect::Complete {
        terminal: AlterClientQuotasTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}

fn submitted_machine() -> AlterClientQuotasMachine {
    let mut machine = machine(20);
    machine
        .apply(AlterClientQuotasInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(AlterClientQuotasInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn machine(deadline: u64) -> AlterClientQuotasMachine {
    AlterClientQuotasMachine::new(
        OperationId::from_raw(49),
        Deadline::from_tick(deadline),
        plan_fixture(),
    )
}

fn plan_fixture() -> AlterClientQuotasPlan {
    AlterClientQuotasPlan::new(
        vec![
            AlterClientQuotaEntry::new(
                user_entity(),
                vec![AlterClientQuotaOperation::set(
                    "producer_byte_rate".to_owned(),
                    1024.0,
                )],
            ),
            AlterClientQuotaEntry::new(
                ip_entity(),
                vec![AlterClientQuotaOperation::remove(
                    "consumer_byte_rate".to_owned(),
                )],
            ),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn user_entity() -> AlterClientQuotaEntity {
    entity(vec![("client-id", Some("app")), ("user", Some("alice"))])
}

fn ip_entity() -> AlterClientQuotaEntity {
    entity(vec![("ip", None)])
}

fn entity(parts: Vec<(&str, Option<&str>)>) -> AlterClientQuotaEntity {
    AlterClientQuotaEntity::new(
        parts
            .into_iter()
            .map(|(entity_type, entity_name)| {
                AlterClientQuotaEntityComponent::new(
                    entity_type.to_owned(),
                    entity_name.map(str::to_owned),
                )
            })
            .collect(),
    )
}
