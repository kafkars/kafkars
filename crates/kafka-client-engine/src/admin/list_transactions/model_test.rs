//! Scenarios for inert engine Admin `ListTransactions` request filters.

use super::AdminListTransactionsRequest;

#[test]
fn request_preserves_exact_filters_until_core_validation() {
    let plan = AdminListTransactionsRequest::new(
        vec!["Ongoing".to_owned(), String::new()],
        vec![i64::MIN, -1, i64::MAX],
        Some(42),
        Some("^orders-".to_owned()),
    )
    .canonicalize()
    .into_plan()
    .unwrap_or_else(|error| panic!("valid plan: {error:?}"));

    assert_eq!(plan.state_filters(), ["Ongoing", ""]);
    assert_eq!(plan.producer_id_filters(), [i64::MIN, -1, i64::MAX]);
    assert_eq!(plan.duration_filter_ms(), Some(42));
    assert_eq!(plan.transactional_id_pattern(), Some("^orders-"));
}

#[test]
fn invalid_shapes_and_unrepresentable_duration_remain_inert() {
    assert!(
        AdminListTransactionsRequest::new(
            vec!["Ongoing".to_owned(), "Ongoing".to_owned()],
            Vec::new(),
            None,
            None,
        )
        .into_plan()
        .is_err()
    );
    assert!(
        AdminListTransactionsRequest::new(Vec::new(), Vec::new(), Some(u64::MAX), None)
            .into_plan()
            .is_err()
    );
}
