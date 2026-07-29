//! Exact signed transaction and broker-error value scenarios.

use core::num::NonZeroI16;

use super::{AdminListTransactionsBrokerError, AdminListedTransaction};

#[test]
fn listed_transaction_preserves_exact_signed_and_string_facts() {
    let transaction =
        AdminListedTransaction::new(String::new(), i64::MIN, "FutureState".to_owned());
    assert_eq!(transaction.transactional_id(), "");
    assert_eq!(transaction.producer_id(), i64::MIN);
    assert_eq!(transaction.transaction_state(), "FutureState");
    assert_eq!(
        transaction.into_parts(),
        (String::new(), i64::MIN, "FutureState".to_owned())
    );
}

#[test]
fn broker_error_preserves_negative_unknown_code() {
    let error = AdminListTransactionsBrokerError::new(
        9,
        NonZeroI16::new(-321).unwrap_or_else(|| panic!("nonzero")),
    );
    assert_eq!(error.broker_id(), 9);
    assert_eq!(error.code(), -321);
    assert_eq!(error.into_parts(), (9, -321));
}
