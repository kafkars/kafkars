//! Public-to-engine transaction-filter translation scenarios.
#![expect(
    clippy::expect_used,
    reason = "the test requires both serialized filter markers"
)]

use std::time::Duration;

use super::ListTransactionsAdminRequest;

#[test]
fn translation_preserves_caller_order_signed_ids_pattern_and_millis() {
    let request = ListTransactionsAdminRequest::new(
        vec!["Ongoing".to_owned(), "Empty".to_owned()],
        vec![9, -1, i64::MIN],
        Some(Duration::from_millis(17)),
        Some("[broker-owned".to_owned()),
    );
    let engine = format!("{:?}", request.into_engine());
    let ongoing = engine.find("\"Ongoing\"").expect("ongoing filter");
    let empty = engine.find("\"Empty\"").expect("empty filter");
    assert!(ongoing < empty);
    assert!(engine.contains("17"));
    assert!(engine.contains("-9223372036854775808"));
    assert!(engine.contains("[broker-owned"));
}

#[test]
fn duration_beyond_u64_millis_remains_invalid_for_engine_validation() {
    let request =
        ListTransactionsAdminRequest::new(Vec::new(), Vec::new(), Some(Duration::MAX), None);
    let engine = format!("{:?}", request.into_engine());
    assert!(engine.contains(&u64::MAX.to_string()));
}
