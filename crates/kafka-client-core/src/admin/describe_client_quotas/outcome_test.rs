//! Stable entity, quota-value, and exact broker-error scenarios.

use core::num::NonZeroI16;

use super::{
    DescribeClientQuotaEntity, DescribeClientQuotaEntityComponent, DescribeClientQuotaValue,
    DescribeClientQuotasBatch, DescribeClientQuotasBrokerError,
};

#[test]
fn stable_result_values_preserve_nullable_names_and_floating_values() {
    let component =
        DescribeClientQuotaEntityComponent::new("user".to_owned(), Some("alice".to_owned()));
    assert_eq!(component.entity_type(), "user");
    assert_eq!(component.entity_name(), Some("alice"));

    let value = DescribeClientQuotaValue::new("producer_byte_rate".to_owned(), 12.5);
    assert_eq!(value.key(), "producer_byte_rate");
    assert_eq!(value.value(), 12.5);

    let batch = DescribeClientQuotasBatch::new(
        7,
        vec![DescribeClientQuotaEntity::new(vec![component], vec![value])],
    );
    assert_eq!(batch.throttle_time_ms(), 7);
    assert_eq!(batch.entities().len(), 1);
}

#[test]
fn broker_error_preserves_exact_signed_code_and_bounded_diagnostic_facts() {
    let error = DescribeClientQuotasBrokerError::new(
        NonZeroI16::new(-29).unwrap_or_else(|| panic!("nonzero")),
        Some("quota store unavailable".to_owned()),
        true,
    );

    assert_eq!(error.code(), -29);
    assert_eq!(error.message(), Some("quota store unavailable"));
    assert!(error.message_truncated());
}
