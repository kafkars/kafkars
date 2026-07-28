//! Engine ACL request ownership, canonicalization, and core validation tests.

use super::{
    DescribeAclsAdmissionError, DescribeAclsAdmissionErrorKind, DescribeAclsFilter,
    DescribeAclsRequest,
};

#[test]
fn request_preserves_exact_nullable_filter_values_in_core_plan() {
    let request = DescribeAclsRequest::new(DescribeAclsFilter::new(
        2,
        Some("orders".to_owned()),
        2,
        Some("User:alice".to_owned()),
        None,
        3,
        1,
    ))
    .canonicalize();

    assert_eq!(request.filter().resource_name(), Some("orders"));
    assert_eq!(request.filter().principal(), Some("User:alice"));
    assert_eq!(request.filter().host(), None);

    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid ACL filter: {error}"));
    let filter = plan.filter();
    assert_eq!(filter.resource_type(), 2);
    assert_eq!(filter.resource_name(), Some("orders"));
    assert_eq!(filter.pattern_type(), 2);
    assert_eq!(filter.principal(), Some("User:alice"));
    assert_eq!(filter.host(), None);
    assert_eq!(filter.operation(), 3);
    assert_eq!(filter.permission_type(), 1);
}

#[test]
fn invalid_exact_or_present_empty_selectors_are_rejected_by_core() {
    for filter in [
        DescribeAclsFilter::new(0, None, 1, None, None, 1, 1),
        DescribeAclsFilter::new(1, Some(String::new()), 1, None, None, 1, 1),
        DescribeAclsFilter::new(1, None, -1, None, None, 1, 1),
        DescribeAclsFilter::new(1, None, 1, Some(String::new()), None, 1, 1),
        DescribeAclsFilter::new(1, None, 1, None, Some(String::new()), 1, 1),
        DescribeAclsFilter::new(1, None, 1, None, None, 0, 1),
        DescribeAclsFilter::new(1, None, 1, None, None, 1, 0),
    ] {
        assert!(DescribeAclsRequest::new(filter).into_plan().is_err());
    }
}

#[test]
fn public_filter_parts_and_admission_error_are_stable() {
    let parts = DescribeAclsRequest::new(DescribeAclsFilter::new(
        7,
        None,
        4,
        Some("User:*".to_owned()),
        Some("*".to_owned()),
        15,
        3,
    ))
    .into_filter()
    .into_parts();
    assert_eq!(
        parts,
        (
            7,
            None,
            4,
            Some("User:*".to_owned()),
            Some("*".to_owned()),
            15,
            3,
        )
    );

    let error = DescribeAclsAdmissionError::new(DescribeAclsAdmissionErrorKind::InvalidRequest);
    assert_eq!(error.kind(), DescribeAclsAdmissionErrorKind::InvalidRequest);
}
