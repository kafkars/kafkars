//! Scenarios for stable API 65 transaction-description facts.

use super::{
    AdminDescribeTransactionDescription, AdminDescribeTransactionTopic,
    DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES,
};

#[test]
fn description_preserves_signed_scalars_optional_start_and_nested_topics() {
    let description = AdminDescribeTransactionDescription::new(
        "Ongoing".to_owned(),
        60_000,
        Some(1_700_000_000_123),
        91,
        7,
        vec![AdminDescribeTransactionTopic::new(
            "orders".to_owned(),
            vec![2, 0],
        )],
    );

    assert_eq!(description.transaction_state(), "Ongoing");
    assert_eq!(description.transaction_timeout_ms(), 60_000);
    assert_eq!(
        description.transaction_start_time_ms(),
        Some(1_700_000_000_123)
    );
    assert_eq!(description.producer_id(), 91);
    assert_eq!(description.producer_epoch(), 7);
    assert_eq!(description.topics()[0].topic(), "orders");
    assert_eq!(description.topics()[0].partitions(), [2, 0]);
}

#[test]
fn scalar_shape_bounds_state_and_normalized_start_without_reclassifying_signed_facts() {
    let signed =
        AdminDescribeTransactionDescription::new("Empty".to_owned(), -1, None, -1, -1, Vec::new());
    assert!(signed.has_bounded_scalar_shape());

    for description in [
        AdminDescribeTransactionDescription::new(String::new(), 1, None, 1, 1, Vec::new()),
        AdminDescribeTransactionDescription::new(
            "x".repeat(DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES + 1),
            1,
            None,
            1,
            1,
            Vec::new(),
        ),
        AdminDescribeTransactionDescription::new(
            "Ongoing".to_owned(),
            1,
            Some(-1),
            1,
            1,
            Vec::new(),
        ),
    ] {
        assert!(!description.has_bounded_scalar_shape());
    }
}
