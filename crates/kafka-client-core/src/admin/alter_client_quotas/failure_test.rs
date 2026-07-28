//! Deadline and delivery-certainty scenarios for Admin `AlterClientQuotas`.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AlterClientQuotaEntity, AlterClientQuotaEntityComponent, AlterClientQuotaEntry,
    AlterClientQuotaOperation, AlterClientQuotasEffect, AlterClientQuotasFailureKind,
    AlterClientQuotasInput, AlterClientQuotasMachine, AlterClientQuotasPlan,
    AlterClientQuotasState, AlterClientQuotasTerminal, AlterClientQuotasTransition,
};

#[test]
fn pre_driver_deadline_and_rejection_are_definitely_unsent() {
    let mut expired = machine(4);
    assert_failure(
        expired
            .apply(AlterClientQuotasInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start should settle: {error}")),
        AlterClientQuotasFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut queued = machine(20);
    queued
        .apply(AlterClientQuotasInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should submit: {error}"));
    assert_failure(
        queued
            .apply(AlterClientQuotasInput::DeadlineElapsed)
            .unwrap_or_else(|error| panic!("queued expiry should settle: {error}")),
        AlterClientQuotasFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine(20);
    rejected
        .apply(AlterClientQuotasInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should submit: {error}"));
    assert_failure(
        rejected
            .apply(AlterClientQuotasInput::DriverRejected)
            .unwrap_or_else(|error| panic!("rejection should settle: {error}")),
        AlterClientQuotasFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn submitted_failures_preserve_delivery_certainty_without_retry() {
    for (input, kind, delivery) in [
        (
            AlterClientQuotasInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            AlterClientQuotasFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            AlterClientQuotasInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            AlterClientQuotasFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            AlterClientQuotasInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            AlterClientQuotasFailureKind::Compatibility,
            DeliveryStatus::PossiblySent,
        ),
        (
            AlterClientQuotasInput::ResponseTooLarge,
            AlterClientQuotasFailureKind::ResponseTooLarge,
            DeliveryStatus::PossiblySent,
        ),
        (
            AlterClientQuotasInput::InvalidResponse,
            AlterClientQuotasFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        let mut machine = submitted_machine();
        let transition = machine
            .apply(input)
            .unwrap_or_else(|error| panic!("failure should settle: {error}"));
        assert_failure(transition, kind, delivery);
        assert_eq!(machine.state(), AlterClientQuotasState::Completed);
    }
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
        AlterClientQuotasPlan::new(
            vec![AlterClientQuotaEntry::new(
                AlterClientQuotaEntity::new(vec![AlterClientQuotaEntityComponent::new(
                    "user".to_owned(),
                    Some("alice".to_owned()),
                )]),
                vec![AlterClientQuotaOperation::remove("quota".to_owned())],
            )],
            false,
        )
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}
