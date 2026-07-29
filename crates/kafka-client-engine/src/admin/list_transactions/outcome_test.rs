//! Lossless core-to-engine Admin `ListTransactions` translation scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminListTransactionsBatch as CoreBatch, AdminListTransactionsBrokerError as CoreBrokerError,
    AdminListTransactionsTerminal as CoreTerminal, AdminListedTransaction as CoreTransaction,
    DescribeClusterBrokerError,
};

use super::{AdminListTransactionsOutcome, outcome::translate_terminal};

#[test]
fn listing_preserves_max_throttle_order_signed_values_and_exact_errors() {
    let terminal = CoreTerminal::Listed(CoreBatch::new(
        73,
        vec!["FutureState".to_owned()],
        vec![CoreTransaction::new(
            "orders-writer".to_owned(),
            i64::MIN,
            "Ongoing".to_owned(),
        )],
        vec![CoreBrokerError::new(
            9,
            NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("nonzero")),
        )],
    ));
    let AdminListTransactionsOutcome::Listed(batch) = translate_terminal(terminal) else {
        panic!("listed terminal expected");
    };
    let (throttle, unknown, transactions, errors) = batch.into_parts();
    assert_eq!(throttle, 73);
    assert_eq!(unknown, ["FutureState"]);
    assert_eq!(
        transactions[0].clone().into_parts(),
        ("orders-writer".to_owned(), i64::MIN, "Ongoing".to_owned())
    );
    assert_eq!(errors[0].into_parts(), (9, -31_999));
}

#[test]
fn discovery_rejection_preserves_exact_signed_diagnostic() {
    let terminal = CoreTerminal::DiscoveryRejected(DescribeClusterBrokerError::new(
        NonZeroI16::new(-17).unwrap_or_else(|| panic!("nonzero")),
        Some("future controller error".to_owned()),
        true,
    ));
    let AdminListTransactionsOutcome::DiscoveryRejected(error) = translate_terminal(terminal)
    else {
        panic!("discovery rejection expected");
    };
    assert_eq!(
        error.into_parts(),
        (-17, Some("future controller error".to_owned()), true)
    );
}
