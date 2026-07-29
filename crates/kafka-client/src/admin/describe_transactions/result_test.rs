//! Public caller-ordered DescribeTransactions result scenarios.

use std::time::Duration;

use crate::{DeliveryStatus, ErrorKind, KafkaError};

use super::{super::BatchResult, DescribeTransactionsResult, TransactionDescription};

#[test]
fn result_preserves_maximum_throttle_caller_order_and_exact_broker_error() {
    let empty = || TransactionDescription::new("Empty".to_owned(), -1, None, -1, -1, Vec::new());
    let result = DescribeTransactionsResult::new(
        Duration::from_millis(17),
        BatchResult::new(vec![
            ("invoice-writer".to_owned(), Ok(empty())),
            (
                "audit-writer".to_owned(),
                Err(KafkaError::new(ErrorKind::Broker, "broker code -731")
                    .with_broker_code(Some(-731))
                    .with_delivery_status(DeliveryStatus::PossiblySent)),
            ),
        ]),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(17));
    let entries = result.transactions().entries();
    assert_eq!(entries[0].0, "invoice-writer");
    assert_eq!(entries[1].0, "audit-writer");
    let error = entries[1].1.as_ref().expect_err("broker rejection");
    assert_eq!(error.broker_code(), Some(-731));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));

    assert_eq!(result.into_transactions().into_entries().len(), 2);
}
