//! Exact transaction-initialization broker failure facts.

use core::num::NonZeroI16;

use super::{TransactionInitializationBrokerCategory, TransactionInitializationBrokerFailure};

#[test]
fn broker_failure_retains_signed_code_and_fencing_category() {
    for (code, category) in [
        (i16::MIN, TransactionInitializationBrokerCategory::Rejected),
        (47, TransactionInitializationBrokerCategory::Fenced),
    ] {
        let code = NonZeroI16::new(code).unwrap_or_else(|| panic!("code must be nonzero"));
        let failure = TransactionInitializationBrokerFailure::new(code, category);
        assert_eq!(failure.code(), code.get());
        assert_eq!(failure.category(), category);
    }
}
