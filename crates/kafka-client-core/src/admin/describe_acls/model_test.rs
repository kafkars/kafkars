//! ACL filter validation and exact-value scenarios.

use super::{DescribeAclsFilter, DescribeAclsPlan, DescribeAclsPlanError};

#[test]
fn plan_preserves_exact_values_and_nullable_filters() {
    let plan = DescribeAclsPlan::new(DescribeAclsFilter::new(
        2,
        Some("orders".to_owned()),
        3,
        None,
        Some("*".to_owned()),
        1,
        1,
    ))
    .unwrap_or_else(|error| panic!("valid filter: {error}"));

    assert_eq!(plan.filter().resource_type(), 2);
    assert_eq!(plan.filter().resource_name(), Some("orders"));
    assert_eq!(plan.filter().pattern_type(), 3);
    assert_eq!(plan.filter().principal(), None);
    assert_eq!(plan.filter().host(), Some("*"));
    assert_eq!(plan.filter().operation(), 1);
    assert_eq!(plan.filter().permission_type(), 1);
}

#[test]
fn plan_rejects_invalid_domains_and_present_empty_strings() {
    for (filter, expected) in [
        (
            filter(0, Some("orders"), 3, None, None, 1, 1),
            DescribeAclsPlanError::InvalidResourceTypeFilter,
        ),
        (
            filter(2, Some(""), 3, None, None, 1, 1),
            DescribeAclsPlanError::EmptyResourceNameFilter,
        ),
        (
            filter(2, None, 0, None, None, 1, 1),
            DescribeAclsPlanError::InvalidPatternTypeFilter,
        ),
        (
            filter(2, None, 3, Some(""), None, 1, 1),
            DescribeAclsPlanError::EmptyPrincipalFilter,
        ),
        (
            filter(2, None, 3, None, Some(""), 1, 1),
            DescribeAclsPlanError::EmptyHostFilter,
        ),
        (
            filter(2, None, 3, None, None, 0, 1),
            DescribeAclsPlanError::InvalidOperationFilter,
        ),
        (
            filter(2, None, 3, None, None, 1, 0),
            DescribeAclsPlanError::InvalidPermissionTypeFilter,
        ),
    ] {
        assert_eq!(DescribeAclsPlan::new(filter), Err(expected));
    }
}

#[test]
fn plan_rejects_present_filters_outside_the_bounded_string_domain() {
    let too_long = "x".repeat(i16::MAX as usize + 1);
    for (filter, expected) in [
        (
            DescribeAclsFilter::new(2, Some(too_long.clone()), 3, None, None, 1, 1),
            DescribeAclsPlanError::ResourceNameFilterTooLong,
        ),
        (
            DescribeAclsFilter::new(2, None, 3, Some(too_long.clone()), None, 1, 1),
            DescribeAclsPlanError::PrincipalFilterTooLong,
        ),
        (
            DescribeAclsFilter::new(2, None, 3, None, Some(too_long.clone()), 1, 1),
            DescribeAclsPlanError::HostFilterTooLong,
        ),
    ] {
        assert_eq!(DescribeAclsPlan::new(filter), Err(expected));
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
) -> DescribeAclsFilter {
    DescribeAclsFilter::new(
        resource_type,
        resource_name.map(str::to_owned),
        pattern_type,
        principal.map(str::to_owned),
        host.map(str::to_owned),
        operation,
        permission_type,
    )
}
