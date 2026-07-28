//! Deterministic `DescribeCluster` lifecycle scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ClusterBroker, ClusterDescription, DescribeClusterBrokerError, DescribeClusterEffect,
    DescribeClusterFailureKind, DescribeClusterInput, DescribeClusterMachine,
    DescribeClusterMachineError, DescribeClusterState, DescribeClusterTerminal,
};

#[test]
fn successful_description_is_terminal_once() {
    let mut machine = machine();
    let start = machine.apply(DescribeClusterInput::Start {
        now: Moment::from_tick(5),
    });
    assert!(matches!(
        start.map(super::DescribeClusterTransition::into_effect),
        Ok(Some(DescribeClusterEffect::Submit { .. }))
    ));
    assert_eq!(
        machine.apply(DescribeClusterInput::DriverAccepted),
        Ok(super::DescribeClusterTransition::none())
    );
    let description = ClusterDescription::new(
        String::from("cluster-a"),
        Some(1),
        vec![ClusterBroker::new(
            1,
            String::from("broker"),
            9092,
            None,
            false,
        )],
    );
    let terminal = machine.apply(DescribeClusterInput::BrokerResponded { description });
    assert!(matches!(
        terminal.map(super::DescribeClusterTransition::into_effect),
        Ok(Some(DescribeClusterEffect::Complete {
            terminal: DescribeClusterTerminal::Cluster(_),
            ..
        }))
    ));
    assert_eq!(machine.state(), DescribeClusterState::Completed);
    assert_eq!(
        machine.apply(DescribeClusterInput::InvalidResponse),
        Err(DescribeClusterMachineError::AlreadyCompleted)
    );
}

#[test]
fn authorization_policy_and_result_cross_the_existing_owner() {
    let mut machine = DescribeClusterMachine::new_with_options(
        OperationId::from_raw(9),
        Deadline::from_tick(10),
        false,
        true,
    );
    let effect = machine
        .apply(DescribeClusterInput::Start {
            now: Moment::from_tick(5),
        })
        .unwrap_or_else(|error| panic!("start machine: {error}"))
        .into_effect();
    assert!(matches!(
        effect,
        Some(DescribeClusterEffect::Submit {
            include_fenced_brokers: false,
            include_authorized_operations: true,
            ..
        })
    ));
    machine
        .apply(DescribeClusterInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept driver ownership: {error}"));
    let description = ClusterDescription::new_with_authorized_operations(
        "cluster-a".to_owned(),
        Some(1),
        Vec::new(),
        Some(0x21),
    );
    let effect = machine
        .apply(DescribeClusterInput::BrokerResponded { description })
        .unwrap_or_else(|error| panic!("settle response: {error}"))
        .into_effect();
    let Some(DescribeClusterEffect::Complete {
        terminal: DescribeClusterTerminal::Cluster(description),
        ..
    }) = effect
    else {
        panic!("cluster response must complete");
    };
    assert_eq!(description.authorized_operations(), Some(0x21));
}

#[test]
fn already_elapsed_start_settles_once_without_driver_ownership() {
    let mut machine = machine();
    let terminal = machine
        .apply(DescribeClusterInput::Start {
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("elapsed start must settle: {error}"))
        .into_effect();
    let Some(DescribeClusterEffect::Complete {
        terminal: DescribeClusterTerminal::Failed(failure),
        ..
    }) = terminal
    else {
        panic!("elapsed start must complete");
    };
    assert_eq!(failure.kind(), DescribeClusterFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
    assert_eq!(machine.state(), DescribeClusterState::Completed);
    assert_eq!(
        machine.apply(DescribeClusterInput::DeadlineElapsed),
        Err(DescribeClusterMachineError::AlreadyCompleted)
    );
}

#[test]
fn signed_broker_error_and_nullable_message_are_terminal_losslessly() {
    let mut machine = machine();
    machine
        .apply(DescribeClusterInput::Start {
            now: Moment::from_tick(5),
        })
        .and_then(|_| machine.apply(DescribeClusterInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("start machine: {error}"));
    let error = DescribeClusterBrokerError::new(
        NonZeroI16::new(-32).unwrap_or_else(|| panic!("nonzero")),
        None,
        false,
    );
    let terminal = machine.apply(DescribeClusterInput::BrokerRejected { error });
    let Ok(Some(DescribeClusterEffect::Complete {
        terminal: DescribeClusterTerminal::BrokerRejected(error),
        ..
    })) = terminal.map(super::DescribeClusterTransition::into_effect)
    else {
        panic!("broker rejection must complete");
    };
    assert_eq!(error.into_parts(), (-32, None, false));
}

#[test]
fn malformed_owned_response_is_possibly_sent() {
    let mut machine = machine();
    machine
        .apply(DescribeClusterInput::Start {
            now: Moment::from_tick(5),
        })
        .and_then(|_| machine.apply(DescribeClusterInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("start machine: {error}"));
    let terminal = machine
        .apply(DescribeClusterInput::InvalidResponse)
        .unwrap_or_else(|error| panic!("finish malformed response: {error}"))
        .into_effect();
    let Some(DescribeClusterEffect::Complete {
        terminal: DescribeClusterTerminal::Failed(failure),
        ..
    }) = terminal
    else {
        panic!("malformed response must fail");
    };
    assert_eq!(failure.kind(), DescribeClusterFailureKind::InvalidResponse);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn explicit_fenced_view_crosses_submit_and_compatibility_is_definitely_unsent() {
    let mut machine = DescribeClusterMachine::new_with_options(
        OperationId::from_raw(9),
        Deadline::from_tick(10),
        true,
        true,
    );
    let effect = machine
        .apply(DescribeClusterInput::Start {
            now: Moment::from_tick(5),
        })
        .unwrap_or_else(|error| panic!("start machine: {error}"))
        .into_effect();
    assert!(matches!(
        effect,
        Some(DescribeClusterEffect::Submit {
            include_fenced_brokers: true,
            include_authorized_operations: true,
            ..
        })
    ));
    machine
        .apply(DescribeClusterInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept driver ownership: {error}"));
    let terminal = machine
        .apply(DescribeClusterInput::ProtocolIncompatible {
            delivery: DeliveryStatus::NotSent,
        })
        .unwrap_or_else(|error| panic!("settle compatibility: {error}"))
        .into_effect();
    let Some(DescribeClusterEffect::Complete {
        terminal: DescribeClusterTerminal::Failed(failure),
        ..
    }) = terminal
    else {
        panic!("compatibility must complete");
    };
    assert_eq!(failure.kind(), DescribeClusterFailureKind::Compatibility);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn authentication_rejection_remains_distinct_and_definitely_unsent() {
    let mut machine = machine();
    machine
        .apply(DescribeClusterInput::Start {
            now: Moment::from_tick(5),
        })
        .and_then(|_| machine.apply(DescribeClusterInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("start machine: {error}"));
    let terminal = machine
        .apply(DescribeClusterInput::AuthenticationFailed {
            delivery: DeliveryStatus::NotSent,
        })
        .unwrap_or_else(|error| panic!("settle authentication rejection: {error}"))
        .into_effect();
    let Some(DescribeClusterEffect::Complete {
        terminal: DescribeClusterTerminal::Failed(failure),
        ..
    }) = terminal
    else {
        panic!("authentication rejection must complete");
    };
    assert_eq!(failure.kind(), DescribeClusterFailureKind::Authentication);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}

fn machine() -> DescribeClusterMachine {
    DescribeClusterMachine::new(OperationId::from_raw(9), Deadline::from_tick(10))
}
