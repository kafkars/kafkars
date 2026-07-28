//! Exact signed errors and allocation-free positional outcome tests.

use core::num::NonZeroI16;

use super::{
    DeleteAclBrokerError, DeleteAclFilterResult, DeleteAclMatchResult, DeleteAclMatchingBinding,
    DeleteAclsBatch, DeleteAclsFilter, DeleteAclsPlan,
};

#[test]
fn signed_error_and_nullable_bounded_diagnostic_remain_exact() {
    let error = DeleteAclBrokerError::new(
        NonZeroI16::new(-731).unwrap_or_else(|| panic!("nonzero")),
        Some("denied".to_owned()),
        true,
    );

    assert_eq!(error.code(), -731);
    assert_eq!(error.message(), Some("denied"));
    assert!(error.message_truncated());
    assert_eq!(error.into_parts(), (-731, Some("denied".to_owned()), true));
}

#[test]
fn positional_outcomes_zip_existing_filter_and_result_vectors() {
    let duplicate = filter();
    let plan = DeleteAclsPlan::new(vec![duplicate.clone(), duplicate])
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let batch = DeleteAclsBatch::from_plan(
        7,
        plan,
        vec![
            DeleteAclFilterResult::Matched(Vec::new()),
            DeleteAclFilterResult::Matched(vec![matching("orders")]),
        ],
    );

    assert_eq!(batch.throttle_time_ms(), 7);
    assert_eq!(batch.outcomes().len(), 2);
    let DeleteAclFilterResult::Matched(matches) = &batch.results()[1] else {
        panic!("matching result");
    };
    assert_eq!(matches[0].resource_name(), "orders");
    assert!(matches!(matches[0].result(), DeleteAclMatchResult::Deleted));
}

fn filter() -> DeleteAclsFilter {
    DeleteAclsFilter::new(1, None, 1, None, None, 1, 1)
}

fn matching(name: &str) -> DeleteAclMatchingBinding {
    DeleteAclMatchingBinding::new(
        2,
        name.to_owned(),
        3,
        "User:alice".to_owned(),
        "*".to_owned(),
        3,
        3,
        DeleteAclMatchResult::Deleted,
    )
}
