//! Ownership-moving resource error scenarios for execution adapters.

use core::num::NonZeroI16;

use super::DescribeConfigBrokerError;

#[test]
fn signed_broker_error_moves_into_adapter_parts_exactly() {
    let code = NonZeroI16::new(-32_123).unwrap_or_else(|| panic!("code is nonzero"));
    let error = DescribeConfigBrokerError::new(code, Some("future error".to_owned()), true);
    assert_eq!(
        error.into_parts(),
        (-32_123, Some("future error".to_owned()), true)
    );
}
