//! Deadline, submission, exact rejection, and delivery-certainty scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeMetadataQuorumBrokerError, DescribeMetadataQuorumDescription,
    DescribeMetadataQuorumEffect, DescribeMetadataQuorumFailure, DescribeMetadataQuorumFailureKind,
    DescribeMetadataQuorumInput, DescribeMetadataQuorumMachine, DescribeMetadataQuorumMachineError,
    DescribeMetadataQuorumPartitionError, DescribeMetadataQuorumState,
    DescribeMetadataQuorumTerminal,
};

#[test]
fn sole_submission_reuses_original_identity_and_absolute_deadline() {
    let mut machine = machine();
    let submit = effect(
        &mut machine,
        DescribeMetadataQuorumInput::Start {
            now: Moment::from_tick(1),
        },
    );

    assert_eq!(
        submit,
        DescribeMetadataQuorumEffect::Submit {
            operation_id: OperationId::from_raw(55),
            deadline: Deadline::from_tick(100),
        }
    );
    assert_eq!(machine.state(), DescribeMetadataQuorumState::AwaitingDriver);
    assert!(
        machine
            .apply(DescribeMetadataQuorumInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accepted: {error}"))
            .into_effect()
            .is_none()
    );
    assert_eq!(machine.state(), DescribeMetadataQuorumState::Submitted);
}

#[test]
fn elapsed_and_driver_rejected_requests_are_definitely_unsent() {
    let elapsed = failure(effect(
        &mut machine(),
        DescribeMetadataQuorumInput::Start {
            now: Moment::from_tick(100),
        },
    ));
    assert_eq!(
        elapsed.kind(),
        DescribeMetadataQuorumFailureKind::DeadlineElapsed
    );
    assert_eq!(elapsed.delivery(), DeliveryStatus::NotSent);

    let mut rejected = awaiting_machine();
    let rejected = failure(effect(
        &mut rejected,
        DescribeMetadataQuorumInput::DriverRejected,
    ));
    assert_eq!(
        rejected.kind(),
        DescribeMetadataQuorumFailureKind::DriverRejected
    );
    assert_eq!(rejected.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn accepted_mechanism_failures_preserve_authoritative_delivery_certainty() {
    let cases = [
        (
            DescribeMetadataQuorumInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::NotSent,
            },
            DescribeMetadataQuorumFailureKind::DeadlineElapsed,
            DeliveryStatus::NotSent,
        ),
        (
            DescribeMetadataQuorumInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            DescribeMetadataQuorumFailureKind::Transport,
            DeliveryStatus::PossiblySent,
        ),
        (
            DescribeMetadataQuorumInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            DescribeMetadataQuorumFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
    ];

    for (input, kind, delivery) in cases {
        let terminal = failure(effect(&mut submitted_machine(), input));
        assert_eq!(terminal.kind(), kind);
        assert_eq!(terminal.delivery(), delivery);
    }

    let too_large = failure(effect(
        &mut submitted_machine(),
        DescribeMetadataQuorumInput::ResponseTooLarge,
    ));
    assert_eq!(
        too_large.kind(),
        DescribeMetadataQuorumFailureKind::ResponseTooLarge
    );
    assert_eq!(too_large.delivery(), DeliveryStatus::PossiblySent);

    let invalid = failure(effect(
        &mut submitted_machine(),
        DescribeMetadataQuorumInput::InvalidResponse,
    ));
    assert_eq!(
        invalid.kind(),
        DescribeMetadataQuorumFailureKind::InvalidResponse
    );
    assert_eq!(invalid.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn success_and_exact_broker_rejections_publish_distinct_terminals() {
    let description = valid_description();
    let described = effect(
        &mut submitted_machine(),
        DescribeMetadataQuorumInput::BrokerResponded {
            description: description.clone(),
        },
    );
    assert_eq!(
        described,
        DescribeMetadataQuorumEffect::Complete {
            operation_id: OperationId::from_raw(55),
            terminal: DescribeMetadataQuorumTerminal::Described(description),
        }
    );

    let broker = DescribeMetadataQuorumBrokerError::new(
        NonZeroI16::new(-41).unwrap_or_else(|| panic!("nonzero")),
        Some("top".to_owned()),
        false,
    );
    assert!(matches!(
        effect(
            &mut submitted_machine(),
            DescribeMetadataQuorumInput::BrokerRejected {
                error: broker.clone()
            },
        ),
        DescribeMetadataQuorumEffect::Complete {
            terminal: DescribeMetadataQuorumTerminal::BrokerRejected(error),
            ..
        } if error == broker
    ));

    let partition = DescribeMetadataQuorumPartitionError::new(
        NonZeroI16::new(3).unwrap_or_else(|| panic!("nonzero")),
        Some("partition".to_owned()),
        true,
    );
    assert!(matches!(
        effect(
            &mut submitted_machine(),
            DescribeMetadataQuorumInput::PartitionRejected {
                error: partition.clone()
            },
        ),
        DescribeMetadataQuorumEffect::Complete {
            terminal: DescribeMetadataQuorumTerminal::PartitionRejected(error),
            ..
        } if error == partition
    ));
}

#[test]
fn every_fact_is_stage_fenced_and_completion_is_final() {
    assert_eq!(
        machine().apply(DescribeMetadataQuorumInput::DriverAccepted),
        Err(DescribeMetadataQuorumMachineError::InvalidState)
    );
    assert_eq!(
        submitted_machine().apply(DescribeMetadataQuorumInput::DriverRejected),
        Err(DescribeMetadataQuorumMachineError::InvalidState)
    );

    let mut completed = machine();
    effect(
        &mut completed,
        DescribeMetadataQuorumInput::Start {
            now: Moment::from_tick(100),
        },
    );
    assert_eq!(
        completed.apply(DescribeMetadataQuorumInput::InvalidResponse),
        Err(DescribeMetadataQuorumMachineError::AlreadyCompleted)
    );
}

fn machine() -> DescribeMetadataQuorumMachine {
    DescribeMetadataQuorumMachine::new(OperationId::from_raw(55), Deadline::from_tick(100))
}

fn awaiting_machine() -> DescribeMetadataQuorumMachine {
    let mut machine = machine();
    effect(
        &mut machine,
        DescribeMetadataQuorumInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
}

fn submitted_machine() -> DescribeMetadataQuorumMachine {
    let mut machine = awaiting_machine();
    machine
        .apply(DescribeMetadataQuorumInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accepted: {error}"));
    machine
}

fn valid_description() -> DescribeMetadataQuorumDescription {
    DescribeMetadataQuorumDescription::new(Some(1), 7, 42, Vec::new(), Vec::new(), None)
        .unwrap_or_else(|error| panic!("description: {error}"))
}

fn effect(
    machine: &mut DescribeMetadataQuorumMachine,
    input: DescribeMetadataQuorumInput,
) -> DescribeMetadataQuorumEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect expected"))
}

fn failure(effect: DescribeMetadataQuorumEffect) -> DescribeMetadataQuorumFailure {
    let DescribeMetadataQuorumEffect::Complete {
        terminal: DescribeMetadataQuorumTerminal::Failed(failure),
        ..
    } = effect
    else {
        panic!("failed terminal expected");
    };
    failure
}
