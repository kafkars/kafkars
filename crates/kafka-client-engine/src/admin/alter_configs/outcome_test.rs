//! Lossless engine terminal translation scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    ConfigAlteration, Deadline, DeliveryStatus, IncrementalAlterConfigBrokerError,
    IncrementalAlterConfigOutcome, IncrementalAlterConfigsBatch as CoreBatch,
    IncrementalAlterConfigsEffect, IncrementalAlterConfigsFailureKind as CoreFailureKind,
    IncrementalAlterConfigsInput, IncrementalAlterConfigsMachine, IncrementalAlterConfigsPlan,
    IncrementalAlterConfigsTerminal as CoreTerminal, Moment, OperationId, TopicConfigAlteration,
};

use super::{
    IncrementalAlterConfigsDeliveryStatus, IncrementalAlterConfigsFailureKind,
    IncrementalAlterConfigsOutcome, outcome::translate_terminal,
};

#[test]
fn throttle_order_exact_signed_code_and_diagnostic_fact_cross_losslessly() {
    let code = NonZeroI16::new(-32_123).unwrap_or_else(|| panic!("code is nonzero"));
    let terminal = CoreTerminal::Configs(CoreBatch::new(
        77,
        vec![
            IncrementalAlterConfigOutcome::altered("orders"),
            IncrementalAlterConfigOutcome::failed(
                "audit",
                IncrementalAlterConfigBrokerError::new(code, Some("future".to_owned()), true),
            ),
        ],
    ));
    let IncrementalAlterConfigsOutcome::Configs(batch) = translate_terminal(terminal) else {
        panic!("config result expected");
    };
    assert_eq!(batch.throttle_time_ms(), 77);
    assert_eq!(batch.topics()[0].topic(), "orders");
    let error = batch.topics()[1]
        .result()
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("broker error expected"));
    assert_eq!(error.code(), -32_123);
    assert_eq!(error.message(), Some("future"));
    assert!(error.message_truncated());
}

#[test]
fn generic_resource_identity_crosses_terminal_translation_losslessly() {
    let terminal = CoreTerminal::Configs(CoreBatch::new(
        5,
        vec![
            IncrementalAlterConfigOutcome::resource_altered(4, "1"),
            IncrementalAlterConfigOutcome::resource_altered(8, "1"),
            IncrementalAlterConfigOutcome::resource_altered(16, "client"),
            IncrementalAlterConfigOutcome::resource_altered(32, "group"),
            IncrementalAlterConfigOutcome::resource_altered(64, "future"),
        ],
    ));
    let IncrementalAlterConfigsOutcome::Configs(batch) = translate_terminal(terminal) else {
        panic!("generic config result expected");
    };
    assert_eq!(
        batch
            .resources()
            .iter()
            .map(|resource| (resource.resource_type(), resource.resource_name()))
            .collect::<Vec<_>>(),
        [
            (4, "1"),
            (8, "1"),
            (16, "client"),
            (32, "group"),
            (64, "future"),
        ]
    );
}

#[test]
fn every_core_failure_category_and_certainty_is_exhaustively_translated() {
    for (core_kind, engine_kind) in [
        (
            CoreFailureKind::DeadlineElapsed,
            IncrementalAlterConfigsFailureKind::DeadlineElapsed,
        ),
        (
            CoreFailureKind::DriverRejected,
            IncrementalAlterConfigsFailureKind::DriverRejected,
        ),
        (
            CoreFailureKind::Transport,
            IncrementalAlterConfigsFailureKind::Transport,
        ),
        (
            CoreFailureKind::InvalidResponse,
            IncrementalAlterConfigsFailureKind::InvalidResponse,
        ),
        (
            CoreFailureKind::ResponseTooLarge,
            IncrementalAlterConfigsFailureKind::ResponseTooLarge,
        ),
        (
            CoreFailureKind::Compatibility,
            IncrementalAlterConfigsFailureKind::Compatibility,
        ),
    ] {
        let (terminal, expected_delivery) = failure_terminal(core_kind);
        let IncrementalAlterConfigsOutcome::Failed(failure) = translate_terminal(terminal) else {
            panic!("failure expected");
        };
        assert_eq!(failure.kind(), engine_kind);
        assert_eq!(failure.delivery(), expected_delivery);
    }
}

fn failure_terminal(
    kind: CoreFailureKind,
) -> (CoreTerminal, IncrementalAlterConfigsDeliveryStatus) {
    let mut machine = IncrementalAlterConfigsMachine::new(
        OperationId::from_raw(1),
        Deadline::from_tick(10),
        IncrementalAlterConfigsPlan::new(
            vec![TopicConfigAlteration::new(
                "orders".to_owned(),
                vec![ConfigAlteration::delete("retention.ms".to_owned())],
            )],
            false,
        )
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );
    let _submission = machine
        .apply(IncrementalAlterConfigsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start machine: {error}"));
    let (input, delivery) = match kind {
        CoreFailureKind::DriverRejected => (
            IncrementalAlterConfigsInput::DriverRejected,
            IncrementalAlterConfigsDeliveryStatus::NotSent,
        ),
        CoreFailureKind::DeadlineElapsed => {
            accept(&mut machine);
            (
                IncrementalAlterConfigsInput::DriverDeadlineElapsed {
                    delivery: DeliveryStatus::PossiblySent,
                },
                IncrementalAlterConfigsDeliveryStatus::PossiblySent,
            )
        }
        CoreFailureKind::Transport => {
            accept(&mut machine);
            (
                IncrementalAlterConfigsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                },
                IncrementalAlterConfigsDeliveryStatus::PossiblySent,
            )
        }
        CoreFailureKind::InvalidResponse => {
            accept(&mut machine);
            (
                IncrementalAlterConfigsInput::InvalidResponse,
                IncrementalAlterConfigsDeliveryStatus::PossiblySent,
            )
        }
        CoreFailureKind::ResponseTooLarge => {
            accept(&mut machine);
            (
                IncrementalAlterConfigsInput::ResponseTooLarge,
                IncrementalAlterConfigsDeliveryStatus::PossiblySent,
            )
        }
        CoreFailureKind::Compatibility => {
            accept(&mut machine);
            (
                IncrementalAlterConfigsInput::ProtocolIncompatible {
                    delivery: DeliveryStatus::PossiblySent,
                },
                IncrementalAlterConfigsDeliveryStatus::PossiblySent,
            )
        }
    };
    let transition = machine
        .apply(input)
        .unwrap_or_else(|error| panic!("settle machine: {error}"));
    let Some(IncrementalAlterConfigsEffect::Complete { terminal, .. }) = transition.into_effect()
    else {
        panic!("terminal expected");
    };
    (terminal, delivery)
}

fn accept(machine: &mut IncrementalAlterConfigsMachine) {
    let _transition = machine
        .apply(IncrementalAlterConfigsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept driver: {error}"));
}
