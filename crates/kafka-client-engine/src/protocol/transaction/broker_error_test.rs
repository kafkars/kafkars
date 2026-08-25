//! Transaction broker-code preservation and fencing-category scenarios.

use super::{TransactionBrokerCategory, broker_error::transaction_broker_error};

#[test]
fn zero_is_success_and_every_nonzero_signed_code_is_preserved() {
    assert_eq!(transaction_broker_error(0), None);
    for code in [1, i16::MAX, -1, i16::MIN] {
        let error =
            transaction_broker_error(code).unwrap_or_else(|| panic!("{code} must be nonzero"));
        assert_eq!(error.code().get(), code);
        assert_eq!(error.category(), TransactionBrokerCategory::Rejected);
    }
}

#[test]
fn only_producer_identity_fencing_codes_are_categorized_as_fenced() {
    for code in [47, 90] {
        let error =
            transaction_broker_error(code).unwrap_or_else(|| panic!("{code} must be nonzero"));
        assert_eq!(error.code().get(), code);
        assert_eq!(error.category(), TransactionBrokerCategory::Fenced);
    }
}

#[test]
fn coordinator_and_access_rejections_remain_distinct_from_fencing() {
    for code in [14, 15, 16] {
        let error =
            transaction_broker_error(code).unwrap_or_else(|| panic!("{code} must be nonzero"));
        assert_eq!(error.category(), TransactionBrokerCategory::Coordinator);
    }
    for code in [31, 53, 58] {
        let error =
            transaction_broker_error(code).unwrap_or_else(|| panic!("{code} must be nonzero"));
        assert_eq!(error.category(), TransactionBrokerCategory::Access);
    }
}
