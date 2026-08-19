//! Version evolution, correlation, deadline, delivery, and terminal scenarios.

#![expect(
    clippy::needless_pass_by_value,
    reason = "test helpers preserve exact failure ownership"
)]

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    UPDATE_FEATURES_DIAGNOSTIC_BYTES, UpdateFeature, UpdateFeatureIntent, UpdateFeatureOutcome,
    UpdateFeatureResult, UpdateFeaturesBatch, UpdateFeaturesBrokerError,
    UpdateFeaturesBrokerResponse, UpdateFeaturesEffect, UpdateFeaturesFailureKind,
    UpdateFeaturesInput, UpdateFeaturesMachine, UpdateFeaturesMachineError, UpdateFeaturesPlan,
    UpdateFeaturesState, UpdateFeaturesTerminal, UpdateFeaturesTransition,
};

#[test]
fn old_feature_results_restore_caller_order_and_preserve_partial_failures() {
    let mut machine = submitted_machine();
    let code = NonZeroI16::new(-32_111).unwrap_or_else(|| panic!("code is nonzero"));
    let response = UpdateFeaturesBrokerResponse::FeatureResults(UpdateFeaturesBatch::new(
        73,
        vec![
            UpdateFeatureOutcome::failed(
                "kraft.version".to_owned(),
                UpdateFeaturesBrokerError::new(code, Some("unsafe".to_owned()), false),
            ),
            UpdateFeatureOutcome::updated("metadata.version".to_owned()),
        ],
    ));
    let transition = machine
        .apply(UpdateFeaturesInput::BrokerResponded { response })
        .unwrap_or_else(|error| panic!("correlated response should settle: {error}"));
    let batch = updated_batch(transition);

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].feature(), "metadata.version");
    assert_eq!(batch.outcomes()[1].feature(), "kraft.version");
    let UpdateFeatureResult::Failed(error) = batch.outcomes()[1].result() else {
        panic!("kraft result must retain its exact broker failure");
    };
    assert_eq!(error.code(), -32_111);
}

#[test]
fn version_two_atomic_success_synthesizes_every_caller_ordered_result() {
    let mut machine = submitted_machine();
    let transition = machine
        .apply(UpdateFeaturesInput::BrokerResponded {
            response: UpdateFeaturesBrokerResponse::AtomicSuccess {
                throttle_time_ms: 17,
            },
        })
        .unwrap_or_else(|error| panic!("atomic response should settle: {error}"));
    let batch = updated_batch(transition);

    assert_eq!(batch.throttle_time_ms(), 17);
    assert_eq!(batch.outcomes().len(), 2);
    assert_eq!(batch.outcomes()[0].feature(), "metadata.version");
    assert_eq!(batch.outcomes()[1].feature(), "kraft.version");
    assert!(
        batch
            .outcomes()
            .iter()
            .all(|outcome| outcome.result() == &UpdateFeatureResult::Updated)
    );
}

#[test]
fn missing_extra_duplicate_unexpected_and_oversized_results_fail_atomically() {
    let code = NonZeroI16::new(1).unwrap_or_else(|| panic!("code is nonzero"));
    let malformed = [
        UpdateFeaturesBatch::new(
            0,
            vec![UpdateFeatureOutcome::updated("metadata.version".to_owned())],
        ),
        UpdateFeaturesBatch::new(
            0,
            vec![
                UpdateFeatureOutcome::updated("metadata.version".to_owned()),
                UpdateFeatureOutcome::updated("kraft.version".to_owned()),
                UpdateFeatureOutcome::updated("extra".to_owned()),
            ],
        ),
        UpdateFeaturesBatch::new(
            0,
            vec![
                UpdateFeatureOutcome::updated("metadata.version".to_owned()),
                UpdateFeatureOutcome::updated("metadata.version".to_owned()),
            ],
        ),
        UpdateFeaturesBatch::new(
            0,
            vec![
                UpdateFeatureOutcome::updated("metadata.version".to_owned()),
                UpdateFeatureOutcome::updated("unexpected".to_owned()),
            ],
        ),
        UpdateFeaturesBatch::new(
            0,
            vec![
                UpdateFeatureOutcome::updated("metadata.version".to_owned()),
                UpdateFeatureOutcome::failed(
                    "kraft.version".to_owned(),
                    UpdateFeaturesBrokerError::new(
                        code,
                        Some("x".repeat(UPDATE_FEATURES_DIAGNOSTIC_BYTES + 1)),
                        false,
                    ),
                ),
            ],
        ),
    ];
    for batch in malformed {
        let mut machine = submitted_machine();
        let transition = machine
            .apply(UpdateFeaturesInput::BrokerResponded {
                response: UpdateFeaturesBrokerResponse::FeatureResults(batch),
            })
            .unwrap_or_else(|error| panic!("malformed response should settle: {error}"));
        assert_failure(
            transition,
            UpdateFeaturesFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        );
    }
}

#[test]
fn top_level_broker_error_is_a_whole_operation_failure() {
    let code = NonZeroI16::new(42).unwrap_or_else(|| panic!("code is nonzero"));
    let mut machine = submitted_machine();
    let transition = machine
        .apply(UpdateFeaturesInput::BrokerRejected {
            error: UpdateFeaturesBrokerError::new(
                code,
                Some("controller rejected batch".to_owned()),
                false,
            ),
        })
        .unwrap_or_else(|error| panic!("broker rejection should settle: {error}"));
    let Some(UpdateFeaturesEffect::Complete {
        terminal: UpdateFeaturesTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("top-level error must fail the whole operation");
    };
    let UpdateFeaturesFailureKind::Broker(error) = failure.kind() else {
        panic!("failure must retain exact top-level broker error");
    };
    assert_eq!(error.code(), 42);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn pre_driver_deadline_and_rejection_are_definitely_unsent() {
    let mut expired = machine(4);
    assert_failure(
        expired
            .apply(UpdateFeaturesInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start should settle: {error}")),
        UpdateFeaturesFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    for (input, kind) in [
        (
            UpdateFeaturesInput::DeadlineElapsed,
            UpdateFeaturesFailureKind::DeadlineElapsed,
        ),
        (
            UpdateFeaturesInput::DriverRejected,
            UpdateFeaturesFailureKind::DriverRejected,
        ),
    ] {
        let mut awaiting = machine(20);
        awaiting
            .apply(UpdateFeaturesInput::Start {
                now: Moment::from_tick(1),
            })
            .unwrap_or_else(|error| panic!("start should submit: {error}"));
        let transition = awaiting
            .apply(input)
            .unwrap_or_else(|error| panic!("pre-driver failure should settle: {error}"));
        assert_failure(transition, kind, DeliveryStatus::NotSent);
    }
}

#[test]
fn submitted_failures_preserve_delivery_and_terminal_assignment_without_retry() {
    for (input, kind, delivery) in [
        (
            UpdateFeaturesInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            UpdateFeaturesFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            UpdateFeaturesInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            UpdateFeaturesFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            UpdateFeaturesInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            UpdateFeaturesFailureKind::Compatibility,
            DeliveryStatus::PossiblySent,
        ),
        (
            UpdateFeaturesInput::ResponseTooLarge,
            UpdateFeaturesFailureKind::ResponseTooLarge,
            DeliveryStatus::PossiblySent,
        ),
        (
            UpdateFeaturesInput::InvalidResponse,
            UpdateFeaturesFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        let mut machine = submitted_machine();
        let transition = machine
            .apply(input)
            .unwrap_or_else(|error| panic!("submitted failure should settle: {error}"));
        assert_failure(transition, kind, delivery);
        assert_eq!(machine.state(), UpdateFeaturesState::Completed);
        assert_eq!(
            machine.apply(UpdateFeaturesInput::InvalidResponse),
            Err(UpdateFeaturesMachineError::AlreadyCompleted)
        );
    }
}

fn updated_batch(transition: UpdateFeaturesTransition) -> UpdateFeaturesBatch {
    let Some(UpdateFeaturesEffect::Complete {
        terminal: UpdateFeaturesTerminal::Updated(batch),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected updated terminal");
    };
    batch
}

fn assert_failure(
    transition: UpdateFeaturesTransition,
    kind: UpdateFeaturesFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(UpdateFeaturesEffect::Complete {
        terminal: UpdateFeaturesTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), &kind);
    assert_eq!(failure.delivery(), delivery);
}

fn submitted_machine() -> UpdateFeaturesMachine {
    let mut machine = machine(20);
    machine
        .apply(UpdateFeaturesInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(UpdateFeaturesInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn machine(deadline: u64) -> UpdateFeaturesMachine {
    UpdateFeaturesMachine::new(
        OperationId::from_raw(57),
        Deadline::from_tick(deadline),
        UpdateFeaturesPlan::new(
            vec![
                UpdateFeature::new(
                    "metadata.version".to_owned(),
                    19,
                    UpdateFeatureIntent::Upgrade,
                ),
                UpdateFeature::new(
                    "kraft.version".to_owned(),
                    1,
                    UpdateFeatureIntent::SafeDowngrade,
                ),
            ],
            true,
        )
        .unwrap_or_else(|error| panic!("valid fixture: {error}")),
    )
}
