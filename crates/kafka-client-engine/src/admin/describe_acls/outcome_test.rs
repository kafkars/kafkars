//! Lossless core-to-engine ACL terminal translation tests.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, DeliveryStatus, DescribeAclBinding as CoreBinding, DescribeAclsBatch as CoreBatch,
    DescribeAclsBrokerError as CoreBrokerError, DescribeAclsEffect as CoreEffect,
    DescribeAclsFilter as CoreFilter, DescribeAclsInput as CoreInput,
    DescribeAclsMachine as CoreMachine, DescribeAclsPlan as CorePlan,
    DescribeAclsTerminal as CoreTerminal, Moment, OperationId,
};

use super::{
    DescribeAclsDeliveryStatus, DescribeAclsFailure, DescribeAclsFailureKind,
    DescribeAclsObserverError, DescribeAclsOutcome, outcome::translate_terminal,
};

#[test]
fn throttle_order_and_every_exact_binding_scalar_cross_losslessly() {
    let terminal = CoreTerminal::Described(CoreBatch::new(
        19,
        vec![CoreBinding::new(
            7,
            "alice".to_owned(),
            4,
            "User:admin".to_owned(),
            "127.0.0.1".to_owned(),
            15,
            3,
        )],
    ));

    let DescribeAclsOutcome::Described(batch) = translate_terminal(terminal) else {
        panic!("described ACL batch expected");
    };
    assert_eq!(batch.throttle_time_ms(), 19);
    assert_eq!(batch.bindings()[0].resource_type(), 7);
    assert_eq!(batch.bindings()[0].resource_name(), "alice");
    assert_eq!(batch.bindings()[0].pattern_type(), 4);
    assert_eq!(batch.bindings()[0].principal(), "User:admin");
    assert_eq!(batch.bindings()[0].host(), "127.0.0.1");
    assert_eq!(batch.bindings()[0].operation(), 15);
    assert_eq!(batch.bindings()[0].permission_type(), 3);

    let (_, bindings) = batch.into_parts();
    assert_eq!(
        bindings
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("binding"))
            .into_parts(),
        (
            7,
            "alice".to_owned(),
            4,
            "User:admin".to_owned(),
            "127.0.0.1".to_owned(),
            15,
            3,
        )
    );
}

#[test]
fn exact_broker_rejection_diagnostic_and_delivery_cross_losslessly() {
    let failure = translate_failure(
        CoreInput::BrokerRejected {
            error: CoreBrokerError::new(
                NonZeroI16::new(-31_777).unwrap_or_else(|| panic!("nonzero")),
                Some("future authorization failure".to_owned()),
                true,
            ),
        },
        true,
    );
    let DescribeAclsFailureKind::Broker(error) = failure.kind() else {
        panic!("broker failure expected");
    };
    assert_eq!(error.code(), -31_777);
    assert_eq!(error.message(), Some("future authorization failure"));
    assert!(error.message_truncated());
    assert_eq!(failure.delivery(), DescribeAclsDeliveryStatus::PossiblySent);
}

#[test]
fn every_mechanism_failure_and_delivery_certainty_is_translated() {
    for (input, submitted, expected_kind, expected_delivery) in [
        (
            CoreInput::DeadlineElapsed,
            false,
            DescribeAclsFailureKind::DeadlineElapsed,
            DescribeAclsDeliveryStatus::NotSent,
        ),
        (
            CoreInput::DriverRejected,
            false,
            DescribeAclsFailureKind::DriverRejected,
            DescribeAclsDeliveryStatus::NotSent,
        ),
        (
            CoreInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            true,
            DescribeAclsFailureKind::DeadlineElapsed,
            DescribeAclsDeliveryStatus::PossiblySent,
        ),
        (
            CoreInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            true,
            DescribeAclsFailureKind::Transport,
            DescribeAclsDeliveryStatus::PossiblySent,
        ),
        (
            CoreInput::ResponseTooLarge,
            true,
            DescribeAclsFailureKind::ResponseTooLarge,
            DescribeAclsDeliveryStatus::PossiblySent,
        ),
        (
            CoreInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            true,
            DescribeAclsFailureKind::Compatibility,
            DescribeAclsDeliveryStatus::NotSent,
        ),
        (
            CoreInput::InvalidResponse,
            true,
            DescribeAclsFailureKind::InvalidResponse,
            DescribeAclsDeliveryStatus::PossiblySent,
        ),
    ] {
        let failure = translate_failure(input, submitted);
        assert_eq!(failure.kind(), &expected_kind);
        assert_eq!(failure.delivery(), expected_delivery);
    }
}

#[test]
fn observer_errors_have_operation_specific_diagnostics() {
    assert_eq!(
        DescribeAclsObserverError::AlreadyObserved.to_string(),
        "Admin DescribeAcls result was already observed"
    );
    assert_eq!(
        DescribeAclsObserverError::Stale.to_string(),
        "Admin DescribeAcls observer is stale"
    );
}

fn translate_failure(input: CoreInput, submitted: bool) -> DescribeAclsFailure {
    let mut machine = CoreMachine::new(
        OperationId::from_raw(41),
        Deadline::from_tick(100),
        CorePlan::new(CoreFilter::new(1, None, 1, None, None, 1, 1))
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );
    let _submission = machine
        .apply(CoreInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start machine: {error}"));
    if submitted {
        machine
            .apply(CoreInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accept driver call: {error}"));
    }
    let effect = machine
        .apply(input)
        .unwrap_or_else(|error| panic!("complete machine: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("terminal expected"));
    let CoreEffect::Complete { terminal, .. } = effect else {
        panic!("completion expected");
    };
    let DescribeAclsOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("failure expected");
    };
    failure
}
