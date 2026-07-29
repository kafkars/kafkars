//! Request-filter validation and exact-value scenarios.

use super::{
    AdminListTransactionsPlan, AdminListTransactionsPlanError,
    LIST_TRANSACTIONS_MAX_FILTER_STATE_BYTES, LIST_TRANSACTIONS_MAX_PRODUCER_ID_FILTERS,
    LIST_TRANSACTIONS_MAX_STATE_FILTERS, LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_PATTERN_BYTES,
};

#[test]
fn plan_preserves_empty_filter_sets_and_exact_signed_values() {
    let plan = AdminListTransactionsPlan::new(
        vec!["Ongoing".to_owned(), String::new()],
        vec![i64::MIN, -1, i64::MAX],
        Some(42),
        Some("^orders-.*$".to_owned()),
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.state_filters(), ["Ongoing", ""]);
    assert_eq!(plan.producer_id_filters(), [i64::MIN, -1, i64::MAX]);
    assert_eq!(plan.duration_filter_ms(), Some(42));
    assert_eq!(plan.transactional_id_pattern(), Some("^orders-.*$"));

    let unfiltered = AdminListTransactionsPlan::new(Vec::new(), Vec::new(), None, None)
        .unwrap_or_else(|error| panic!("unfiltered plan: {error}"));
    assert!(unfiltered.state_filters().is_empty());
    assert!(unfiltered.producer_id_filters().is_empty());
    assert_eq!(unfiltered.duration_filter_ms(), None);
    assert_eq!(unfiltered.transactional_id_pattern(), None);
}

#[test]
fn plan_rejects_noncanonical_or_invalid_filters() {
    assert_eq!(
        AdminListTransactionsPlan::new(
            vec!["Ongoing".to_owned(), "Ongoing".to_owned()],
            Vec::new(),
            None,
            None,
        ),
        Err(AdminListTransactionsPlanError::DuplicateStateFilter)
    );
    assert_eq!(
        AdminListTransactionsPlan::new(Vec::new(), vec![-7, -7], None, None),
        Err(AdminListTransactionsPlanError::DuplicateProducerIdFilter)
    );
    assert_eq!(
        AdminListTransactionsPlan::new(Vec::new(), Vec::new(), Some(-1), None),
        Err(AdminListTransactionsPlanError::NegativeDurationFilter)
    );
    assert_eq!(
        AdminListTransactionsPlan::new(Vec::new(), Vec::new(), None, Some(String::new())),
        Err(AdminListTransactionsPlanError::EmptyTransactionalIdPattern)
    );
}

#[test]
fn plan_enforces_each_explicit_request_bound() {
    assert_eq!(
        AdminListTransactionsPlan::new(
            (0..=LIST_TRANSACTIONS_MAX_STATE_FILTERS)
                .map(|index| index.to_string())
                .collect(),
            Vec::new(),
            None,
            None,
        ),
        Err(AdminListTransactionsPlanError::TooManyStateFilters)
    );
    assert_eq!(
        AdminListTransactionsPlan::new(
            vec!["x".repeat(LIST_TRANSACTIONS_MAX_FILTER_STATE_BYTES + 1)],
            Vec::new(),
            None,
            None,
        ),
        Err(AdminListTransactionsPlanError::StateFilterTooLong)
    );
    assert_eq!(
        AdminListTransactionsPlan::new(
            Vec::new(),
            (0..=LIST_TRANSACTIONS_MAX_PRODUCER_ID_FILTERS)
                .map(|producer_id| producer_id as i64)
                .collect(),
            None,
            None,
        ),
        Err(AdminListTransactionsPlanError::TooManyProducerIdFilters)
    );
    assert_eq!(
        AdminListTransactionsPlan::new(
            Vec::new(),
            Vec::new(),
            None,
            Some("x".repeat(LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_PATTERN_BYTES + 1,)),
        ),
        Err(AdminListTransactionsPlanError::TransactionalIdPatternTooLong)
    );
}
