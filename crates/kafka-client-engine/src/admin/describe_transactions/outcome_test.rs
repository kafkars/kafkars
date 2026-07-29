//! Scenarios for lossless core-to-engine Admin `DescribeTransactions` translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminDescribeTransactionBrokerError as CoreBrokerError,
    AdminDescribeTransactionDescription as CoreDescription,
    AdminDescribeTransactionOutcome as CoreOutcome, AdminDescribeTransactionTopic as CoreTopic,
    AdminDescribeTransactionsBatch as CoreBatch, AdminDescribeTransactionsEffect as CoreEffect,
    AdminDescribeTransactionsInput as CoreInput, AdminDescribeTransactionsMachine as CoreMachine,
    AdminDescribeTransactionsPlan as CorePlan, AdminDescribeTransactionsTerminal as CoreTerminal,
    Deadline, DeliveryStatus, Moment, OperationId,
};

use super::{
    AdminDescribeTransactionsDeliveryStatus, AdminDescribeTransactionsFailureKind,
    AdminDescribeTransactionsOutcome, outcome::translate_terminal,
};

#[test]
fn throttle_order_description_and_exact_broker_error_translate() {
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let terminal = CoreTerminal::Described(CoreBatch::new(
        73,
        vec![
            CoreOutcome::described(
                "orders-writer".to_owned(),
                CoreDescription::new(
                    "Ongoing".to_owned(),
                    60_000,
                    Some(1_700_000_000_123),
                    41,
                    3,
                    vec![CoreTopic::new("orders".to_owned(), vec![0, 2])],
                ),
            ),
            CoreOutcome::broker_failed("audit-writer".to_owned(), CoreBrokerError::new(code)),
        ],
    ));
    let AdminDescribeTransactionsOutcome::Described(batch) = translate_terminal(terminal) else {
        panic!("described batch expected");
    };
    let (throttle_time_ms, results) = batch.into_parts();
    assert_eq!(throttle_time_ms, 73);
    let (transactional_id, described) = results[0].clone().into_parts();
    assert_eq!(transactional_id, "orders-writer");
    let description = described.unwrap_or_else(|error| panic!("description expected: {error:?}"));
    let (state, timeout, start, producer_id, epoch, topics) = description.into_parts();
    assert_eq!(
        (state.as_str(), timeout, start, producer_id, epoch),
        ("Ongoing", 60_000, Some(1_700_000_000_123), 41, 3)
    );
    assert_eq!(
        topics[0].clone().into_parts(),
        ("orders".to_owned(), vec![0, 2])
    );
    let (transactional_id, failed) = results[1].clone().into_parts();
    assert_eq!(transactional_id, "audit-writer");
    assert_eq!(
        failed
            .err()
            .unwrap_or_else(|| panic!("broker error expected"))
            .code(),
        -31_999
    );
}

#[test]
fn whole_failure_and_delivery_translate_without_reclassification() {
    let terminal = failed_terminal(CoreInput::ProtocolIncompatible {
        delivery: DeliveryStatus::NotSent,
    });
    let AdminDescribeTransactionsOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("whole-operation failure expected");
    };
    assert_eq!(
        failure.kind(),
        AdminDescribeTransactionsFailureKind::Compatibility
    );
    assert_eq!(
        failure.delivery(),
        AdminDescribeTransactionsDeliveryStatus::NotSent
    );
}

fn failed_terminal(input: CoreInput) -> CoreTerminal {
    let plan = CorePlan::new(vec!["orders-writer".to_owned()])
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let mut machine = CoreMachine::new(OperationId::from_raw(29), Deadline::from_tick(20), plan);
    machine
        .apply(CoreInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(CoreInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit core machine: {error}"));
    let transition = machine
        .apply(input)
        .unwrap_or_else(|error| panic!("terminal input: {error}"));
    let Some(CoreEffect::Complete { terminal, .. }) = transition.into_effect() else {
        panic!("terminal effect expected");
    };
    terminal
}
