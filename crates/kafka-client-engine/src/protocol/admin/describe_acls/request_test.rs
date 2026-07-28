//! Focused evidence for bounded generated ACL filter construction.

use super::{
    DescribeAclsFilterRef, describe_acls_request, request::DescribeAclsRequestFailure,
    retention::MAX_FILTER_STRING_BYTES,
};

#[test]
fn request_preserves_nullable_text_and_exact_signed_codes() {
    let request = describe_acls_request(
        DescribeAclsFilterRef::new(
            -1,
            Some("orders"),
            -2,
            Some("User:alice"),
            Some("127.0.0.1"),
            -3,
            -4,
        ),
        usize::MAX,
    )
    .expect("bounded filter");

    assert_eq!(request.resource_type_filter, -1);
    assert_eq!(request.resource_name_filter.as_deref(), Some("orders"));
    assert_eq!(request.pattern_type_filter, -2);
    assert_eq!(request.principal_filter.as_deref(), Some("User:alice"));
    assert_eq!(request.host_filter.as_deref(), Some("127.0.0.1"));
    assert_eq!(request.operation, -3);
    assert_eq!(request.permission_type, -4);
    assert!(request.unknown_tagged_fields.is_empty());
}

#[test]
fn all_nullable_filters_require_no_retained_text_capacity() {
    let request =
        describe_acls_request(DescribeAclsFilterRef::new(1, None, 3, None, None, 2, 2), 0)
            .expect("wildcard filter");

    assert_eq!(request.resource_name_filter, None);
    assert_eq!(request.principal_filter, None);
    assert_eq!(request.host_filter, None);
}

#[test]
fn request_rejects_empty_present_filters() {
    let resource = describe_acls_request(
        DescribeAclsFilterRef::new(1, Some(""), 3, None, None, 2, 2),
        usize::MAX,
    );
    assert_eq!(resource, Err(DescribeAclsRequestFailure::EmptyResourceName));

    let principal = describe_acls_request(
        DescribeAclsFilterRef::new(1, None, 3, Some(""), None, 2, 2),
        usize::MAX,
    );
    assert_eq!(principal, Err(DescribeAclsRequestFailure::EmptyPrincipal));

    let host = describe_acls_request(
        DescribeAclsFilterRef::new(1, None, 3, None, Some(""), 2, 2),
        usize::MAX,
    );
    assert_eq!(host, Err(DescribeAclsRequestFailure::EmptyHost));
}

#[test]
fn request_rejects_classic_string_overflow() {
    let oversized = "x".repeat(MAX_FILTER_STRING_BYTES + 1);
    let result = describe_acls_request(
        DescribeAclsFilterRef::new(1, Some(&oversized), 3, None, None, 2, 2),
        usize::MAX,
    );

    assert_eq!(
        result,
        Err(DescribeAclsRequestFailure::ResourceNameTooLong {
            actual: MAX_FILTER_STRING_BYTES + 1,
            max: MAX_FILTER_STRING_BYTES,
        })
    );
}

#[test]
fn request_checks_complete_retained_text_before_copying() {
    let filter =
        DescribeAclsFilterRef::new(1, Some("orders"), 3, Some("User:alice"), Some("*"), 2, 3);
    let required = "orders".len() + "User:alice".len() + "*".len();

    assert_eq!(
        describe_acls_request(filter, required - 1),
        Err(DescribeAclsRequestFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    assert!(describe_acls_request(filter, required).is_ok());
}
