//! Exact binding and bounded top-level broker-error scenarios.

use core::num::NonZeroI16;

use super::{DescribeAclBinding, DescribeAclsBrokerError};

#[test]
fn binding_and_error_preserve_exact_signed_protocol_facts() {
    let binding = DescribeAclBinding::new(
        7,
        "orders".to_owned(),
        4,
        "User:alice".to_owned(),
        "*".to_owned(),
        12,
        3,
    );
    assert_eq!(
        binding.into_parts(),
        (
            7,
            "orders".to_owned(),
            4,
            "User:alice".to_owned(),
            "*".to_owned(),
            12,
            3,
        )
    );

    let error = DescribeAclsBrokerError::new(
        NonZeroI16::new(-17).unwrap_or_else(|| panic!("nonzero")),
        Some("denied".to_owned()),
        true,
    );
    assert_eq!(error.code(), -17);
    assert_eq!(error.message(), Some("denied"));
    assert!(error.message_truncated());
}
