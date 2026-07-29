//! Public-to-engine Admin `DescribeTransactions` request translation scenarios.

use super::DescribeTransactionsAdminRequest;

#[test]
fn translation_is_deferred_and_preserves_caller_order() {
    let request = DescribeTransactionsAdminRequest::new(vec![
        "invoice-writer".to_owned(),
        "audit-writer".to_owned(),
    ]);
    let engine = format!("{:?}", request.into_engine());

    let invoice = engine.find("invoice-writer").expect("invoice ID");
    let audit = engine.find("audit-writer").expect("audit ID");
    assert!(invoice < audit);
}
