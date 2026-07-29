//! Stable generated-free Admin transaction-description scenarios.

use super::{TransactionDescription, TransactionTopic};

#[test]
fn description_preserves_signed_scalars_absence_and_nested_partition_order() {
    let description = TransactionDescription::new(
        "Empty".to_owned(),
        -1,
        None,
        -1,
        -1,
        vec![TransactionTopic::new("orders".to_owned(), vec![0, 2])],
    );

    assert_eq!(description.transaction_state(), "Empty");
    assert_eq!(description.transaction_timeout_ms(), -1);
    assert_eq!(description.transaction_start_time_ms(), None);
    assert_eq!(description.producer_id(), -1);
    assert_eq!(description.producer_epoch(), -1);
    assert_eq!(description.topics()[0].topic(), "orders");
    assert_eq!(description.topics()[0].partitions(), [0, 2]);
}

#[test]
fn represented_start_time_and_exact_state_spelling_remain_visible() {
    let description = TransactionDescription::new(
        "PrepareCommit".to_owned(),
        60_000,
        Some(1_700_000_000_123),
        91,
        7,
        Vec::new(),
    );

    assert_eq!(description.transaction_state(), "PrepareCommit");
    assert_eq!(
        description.transaction_start_time_ms(),
        Some(1_700_000_000_123)
    );
}
