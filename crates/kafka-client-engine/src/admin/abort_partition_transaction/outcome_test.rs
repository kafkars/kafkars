//! Exhaustive terminal-translation tests for one partition transaction abort.

use core::num::NonZeroI16;

use kafka_client_core::{
    AbortPartitionTransactionBrokerError as CoreBrokerError,
    AbortPartitionTransactionEffect as CoreEffect, AbortPartitionTransactionInput as CoreInput,
    AbortPartitionTransactionMachine as CoreMachine, AbortPartitionTransactionPlan as CorePlan,
    AbortPartitionTransactionTerminal as CoreTerminal, Deadline, DeliveryStatus, Moment,
    OperationId,
};

use super::{
    AbortPartitionTransactionDeliveryStatus, AbortPartitionTransactionFailureKind,
    AbortPartitionTransactionOutcome, outcome::translate_terminal,
};

#[test]
fn preserves_success_and_exact_signed_broker_code() {
    assert_eq!(
        translate_terminal(CoreTerminal::Aborted),
        AbortPartitionTransactionOutcome::Aborted
    );

    let error = CoreBrokerError::new(NonZeroI16::new(-73).unwrap_or_else(|| panic!("nonzero")));
    let AbortPartitionTransactionOutcome::BrokerRejected(error) =
        translate_terminal(CoreTerminal::BrokerRejected(error))
    else {
        panic!("expected broker rejection");
    };
    assert_eq!(error.code(), -73);
}

#[test]
fn preserves_failure_kind_and_delivery_certainty() {
    let mut machine = CoreMachine::new(
        OperationId::from_raw(1),
        Deadline::from_tick(10),
        CorePlan::new("orders".to_owned(), 3, 41, 7, 11)
            .unwrap_or_else(|error| panic!("valid plan: {error:?}")),
    );
    let _submit = machine
        .apply(CoreInput::Start {
            now: Moment::from_tick(0),
        })
        .unwrap_or_else(|error| panic!("start: {error:?}"));
    let _accepted = machine
        .apply(CoreInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error:?}"));
    let complete = machine
        .apply(CoreInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("fail: {error:?}"));
    let Some(CoreEffect::Complete { terminal, .. }) = complete.into_effect() else {
        panic!("expected terminal");
    };
    let AbortPartitionTransactionOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("expected execution failure");
    };
    assert_eq!(
        failure.kind(),
        AbortPartitionTransactionFailureKind::Transport
    );
    assert_eq!(
        failure.delivery(),
        AbortPartitionTransactionDeliveryStatus::PossiblySent
    );
}
