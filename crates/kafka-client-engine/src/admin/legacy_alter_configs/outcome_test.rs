//! Lossless engine terminal translation scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, DeliveryStatus, LegacyAlterConfigBrokerError, LegacyAlterConfigOutcome,
    LegacyAlterConfigsBatch as CoreBatch, LegacyAlterConfigsEffect,
    LegacyAlterConfigsFailureKind as CoreFailureKind, LegacyAlterConfigsInput,
    LegacyAlterConfigsMachine, LegacyAlterConfigsPlan, LegacyAlterConfigsTerminal as CoreTerminal,
    LegacyConfigEntry, LegacyTopicConfigReplacement, Moment, OperationId,
};

use super::{
    LegacyAlterConfigsDeliveryStatus, LegacyAlterConfigsFailureKind, LegacyAlterConfigsOutcome,
    outcome::translate_terminal,
};

#[test]
fn throttle_order_exact_signed_code_and_diagnostic_fact_cross_losslessly() {
    let code = NonZeroI16::new(-32_123).unwrap_or_else(|| panic!("code is nonzero"));
    let terminal = CoreTerminal::Configs(CoreBatch::new(
        77,
        vec![
            LegacyAlterConfigOutcome::altered("orders"),
            LegacyAlterConfigOutcome::failed(
                "audit",
                LegacyAlterConfigBrokerError::new(code, Some("future".to_owned()), true),
            ),
        ],
    ));
    let LegacyAlterConfigsOutcome::Configs(batch) = translate_terminal(terminal) else {
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
fn exact_resource_identity_crosses_engine_translation_without_reclassification() {
    let terminal = CoreTerminal::Configs(CoreBatch::new(
        23,
        vec![
            LegacyAlterConfigOutcome::resource_altered(4, "1"),
            LegacyAlterConfigOutcome::resource_failed(
                64,
                "future-resource",
                LegacyAlterConfigBrokerError::new(
                    NonZeroI16::new(-30_001).unwrap_or_else(|| panic!("nonzero")),
                    None,
                    false,
                ),
            ),
        ],
    ));
    let LegacyAlterConfigsOutcome::Configs(batch) = translate_terminal(terminal) else {
        panic!("config result expected");
    };

    assert_eq!(batch.resources()[0].resource_type(), 4);
    assert_eq!(batch.resources()[0].resource_name(), "1");
    assert_eq!(batch.resources()[1].resource_type(), 64);
    assert_eq!(batch.resources()[1].resource_name(), "future-resource");
    assert_eq!(
        batch.resources()[1]
            .result()
            .as_ref()
            .err()
            .map(|error| error.code()),
        Some(-30_001)
    );
}

#[test]
fn every_core_failure_category_and_certainty_is_exhaustively_translated() {
    for (core_kind, engine_kind) in [
        (
            CoreFailureKind::DeadlineElapsed,
            LegacyAlterConfigsFailureKind::DeadlineElapsed,
        ),
        (
            CoreFailureKind::DriverRejected,
            LegacyAlterConfigsFailureKind::DriverRejected,
        ),
        (
            CoreFailureKind::Transport,
            LegacyAlterConfigsFailureKind::Transport,
        ),
        (
            CoreFailureKind::InvalidResponse,
            LegacyAlterConfigsFailureKind::InvalidResponse,
        ),
        (
            CoreFailureKind::ResponseTooLarge,
            LegacyAlterConfigsFailureKind::ResponseTooLarge,
        ),
        (
            CoreFailureKind::Compatibility,
            LegacyAlterConfigsFailureKind::Compatibility,
        ),
    ] {
        let (terminal, expected_delivery) = failure_terminal(core_kind);
        let LegacyAlterConfigsOutcome::Failed(failure) = translate_terminal(terminal) else {
            panic!("failure expected");
        };
        assert_eq!(failure.kind(), engine_kind);
        assert_eq!(failure.delivery(), expected_delivery);
    }
}

fn failure_terminal(kind: CoreFailureKind) -> (CoreTerminal, LegacyAlterConfigsDeliveryStatus) {
    let mut machine = LegacyAlterConfigsMachine::new(
        OperationId::from_raw(1),
        Deadline::from_tick(10),
        LegacyAlterConfigsPlan::new(
            vec![LegacyTopicConfigReplacement::new(
                "orders".to_owned(),
                vec![LegacyConfigEntry::new("retention.ms".to_owned(), None)],
            )],
            false,
        )
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );
    let _submission = machine
        .apply(LegacyAlterConfigsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start machine: {error}"));
    let (input, delivery) = match kind {
        CoreFailureKind::DriverRejected => (
            LegacyAlterConfigsInput::DriverRejected,
            LegacyAlterConfigsDeliveryStatus::NotSent,
        ),
        CoreFailureKind::DeadlineElapsed => {
            accept(&mut machine);
            (
                LegacyAlterConfigsInput::DriverDeadlineElapsed {
                    delivery: DeliveryStatus::PossiblySent,
                },
                LegacyAlterConfigsDeliveryStatus::PossiblySent,
            )
        }
        CoreFailureKind::Transport => {
            accept(&mut machine);
            (
                LegacyAlterConfigsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                },
                LegacyAlterConfigsDeliveryStatus::PossiblySent,
            )
        }
        CoreFailureKind::InvalidResponse => {
            accept(&mut machine);
            (
                LegacyAlterConfigsInput::InvalidResponse,
                LegacyAlterConfigsDeliveryStatus::PossiblySent,
            )
        }
        CoreFailureKind::ResponseTooLarge => {
            accept(&mut machine);
            (
                LegacyAlterConfigsInput::ResponseTooLarge,
                LegacyAlterConfigsDeliveryStatus::PossiblySent,
            )
        }
        CoreFailureKind::Compatibility => {
            accept(&mut machine);
            (
                LegacyAlterConfigsInput::ProtocolIncompatible {
                    delivery: DeliveryStatus::PossiblySent,
                },
                LegacyAlterConfigsDeliveryStatus::PossiblySent,
            )
        }
    };
    let transition = machine
        .apply(input)
        .unwrap_or_else(|error| panic!("settle machine: {error}"));
    let Some(LegacyAlterConfigsEffect::Complete { terminal, .. }) = transition.into_effect() else {
        panic!("terminal expected");
    };
    (terminal, delivery)
}

fn accept(machine: &mut LegacyAlterConfigsMachine) {
    let _transition = machine
        .apply(LegacyAlterConfigsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept driver: {error}"));
}
