//! Bounded filter validation and duplicate-position preservation tests.

use super::{DeleteAclsFilter, DeleteAclsPlan, DeleteAclsPlanError, MAX_DELETE_ACLS_FILTERS};

#[test]
fn duplicate_filters_are_semantically_distinct_positions_and_remain_ordered() {
    let duplicate = filter(2, Some("orders"), 3, Some("User:alice"), Some("*"), 3, 3);
    let plan = DeleteAclsPlan::new(vec![duplicate.clone(), duplicate])
        .unwrap_or_else(|error| panic!("duplicates are valid positions: {error}"));

    assert_eq!(plan.required_filter_result_capacity(), 2);
    assert_eq!(plan.filters()[0].resource_name(), Some("orders"));
    assert_eq!(plan.filters()[0], plan.filters()[1]);
}

#[test]
fn batch_is_nonempty_and_bounded() {
    assert_eq!(
        DeleteAclsPlan::new(Vec::new()),
        Err(DeleteAclsPlanError::EmptyBatch)
    );
    let filters = (0..=MAX_DELETE_ACLS_FILTERS)
        .map(|_| DeleteAclsFilter::new(1, None, 1, None, None, 1, 1))
        .collect();
    assert_eq!(
        DeleteAclsPlan::new(filters),
        Err(DeleteAclsPlanError::BatchTooLarge)
    );
}

#[test]
fn unknown_filter_scalars_are_rejected_but_future_positive_codes_survive() {
    for (filter, expected) in [
        (
            filter(0, None, 1, None, None, 1, 1),
            DeleteAclsPlanError::InvalidResourceType,
        ),
        (
            filter(1, None, 0, None, None, 1, 1),
            DeleteAclsPlanError::InvalidPatternType,
        ),
        (
            filter(1, None, 1, None, None, 0, 1),
            DeleteAclsPlanError::InvalidOperation,
        ),
        (
            filter(1, None, 1, None, None, 1, 0),
            DeleteAclsPlanError::InvalidPermissionType,
        ),
    ] {
        assert_eq!(DeleteAclsPlan::new(vec![filter]), Err(expected));
    }
    let future = DeleteAclsPlan::new(vec![filter(101, None, 102, None, None, 103, 104)])
        .unwrap_or_else(|error| panic!("positive future filter codes: {error}"));
    assert_eq!(future.filters()[0].resource_type(), 101);
}

#[test]
fn every_present_filter_string_is_nonempty_and_bounded() {
    let oversized = "x".repeat(i16::MAX as usize + 1);
    for (filter, expected) in [
        (
            filter(1, Some(""), 1, None, None, 1, 1),
            DeleteAclsPlanError::EmptyResourceName,
        ),
        (
            filter(1, Some(&oversized), 1, None, None, 1, 1),
            DeleteAclsPlanError::ResourceNameTooLong,
        ),
        (
            filter(1, None, 1, Some(""), None, 1, 1),
            DeleteAclsPlanError::EmptyPrincipal,
        ),
        (
            filter(1, None, 1, Some(&oversized), None, 1, 1),
            DeleteAclsPlanError::PrincipalTooLong,
        ),
        (
            filter(1, None, 1, None, Some(""), 1, 1),
            DeleteAclsPlanError::EmptyHost,
        ),
        (
            filter(1, None, 1, None, Some(&oversized), 1, 1),
            DeleteAclsPlanError::HostTooLong,
        ),
    ] {
        assert_eq!(DeleteAclsPlan::new(vec![filter]), Err(expected));
    }
}

fn filter(
    resource_type: i8,
    resource_name: Option<&str>,
    pattern_type: i8,
    principal: Option<&str>,
    host: Option<&str>,
    operation: i8,
    permission_type: i8,
) -> DeleteAclsFilter {
    DeleteAclsFilter::new(
        resource_type,
        resource_name.map(str::to_owned),
        pattern_type,
        principal.map(str::to_owned),
        host.map(str::to_owned),
        operation,
        permission_type,
    )
}
