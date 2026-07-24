//! Ownership-moving broker failure scenarios for incremental configuration.

use core::num::NonZeroI16;

use super::IncrementalAlterConfigBrokerError;

#[test]
fn exact_signed_code_and_bounded_diagnostic_fact_move_without_reclassification() {
    let code = NonZeroI16::new(-32_123).unwrap_or_else(|| panic!("code is nonzero"));
    let error = IncrementalAlterConfigBrokerError::new(code, Some("future error".to_owned()), true);

    assert_eq!(error.code(), -32_123);
    assert_eq!(error.message(), Some("future error"));
    assert!(error.message_truncated());
    assert_eq!(
        error.into_parts(),
        (-32_123, Some("future error".to_owned()), true)
    );
}
