//! Focused evidence for bounded caller-ordered ACL deletion filters.

use kafka_wire::RetainedSize;

use super::{
    DeleteAclsFilterRef, DeleteAclsRequestFailure, delete_acls_request,
    retention::{MAX_FILTER_STRING_BYTES, MAX_FILTERS, request_peak_charge},
};

#[test]
fn request_preserves_order_nullable_selectors_and_duplicate_positions() {
    let filters = [
        filter(1, None, 1, None, None, 1, 1),
        filter(2, Some("orders"), 3, Some("User:alice"), Some("*"), 3, 3),
        filter(1, None, 1, None, None, 1, 1),
    ];

    let request = delete_acls_request(&filters, usize::MAX).expect("valid filters");

    assert_eq!(request.filters.len(), 3);
    assert_eq!(request.filters[0], request.filters[2]);
    assert_eq!(request.filters[0].resource_type_filter, 1);
    assert_eq!(request.filters[0].resource_name_filter, None);
    assert_eq!(request.filters[0].pattern_type_filter, 1);
    assert_eq!(request.filters[0].principal_filter, None);
    assert_eq!(request.filters[0].host_filter, None);
    assert_eq!(request.filters[0].operation, 1);
    assert_eq!(request.filters[0].permission_type, 1);

    let concrete = &request.filters[1];
    assert_eq!(concrete.resource_name_filter.as_deref(), Some("orders"));
    assert_eq!(concrete.principal_filter.as_deref(), Some("User:alice"));
    assert_eq!(concrete.host_filter.as_deref(), Some("*"));
    assert!(request.unknown_tagged_fields.is_empty());
    assert!(
        request
            .filters
            .iter()
            .all(|filter| filter.unknown_tagged_fields.is_empty())
    );
}

#[test]
fn request_rejects_empty_oversized_and_invalid_filter_shapes() {
    assert_eq!(
        delete_acls_request(&[], usize::MAX),
        Err(DeleteAclsRequestFailure::EmptyBatch)
    );
    let too_many = vec![any_filter(); MAX_FILTERS + 1];
    assert_eq!(
        delete_acls_request(&too_many, usize::MAX),
        Err(DeleteAclsRequestFailure::TooManyFilters {
            actual: MAX_FILTERS + 1,
            max: MAX_FILTERS,
        })
    );
    assert_eq!(
        delete_acls_request(&[filter(0, None, 1, None, None, 1, 1)], usize::MAX,),
        Err(DeleteAclsRequestFailure::InvalidResourceType { actual: 0 })
    );
    assert_eq!(
        delete_acls_request(&[filter(1, None, -1, None, None, 1, 1)], usize::MAX,),
        Err(DeleteAclsRequestFailure::InvalidPatternType { actual: -1 })
    );
    assert_eq!(
        delete_acls_request(&[filter(1, None, 1, None, None, 0, 1)], usize::MAX,),
        Err(DeleteAclsRequestFailure::InvalidOperation { actual: 0 })
    );
    assert_eq!(
        delete_acls_request(&[filter(1, None, 1, None, None, 1, -2)], usize::MAX,),
        Err(DeleteAclsRequestFailure::InvalidPermissionType { actual: -2 })
    );
}

#[test]
fn request_rejects_present_empty_and_oversized_filter_text() {
    assert_eq!(
        delete_acls_request(&[filter(1, Some(""), 1, None, None, 1, 1)], usize::MAX,),
        Err(DeleteAclsRequestFailure::EmptyResourceName)
    );
    assert_eq!(
        delete_acls_request(&[filter(1, None, 1, Some(""), None, 1, 1)], usize::MAX,),
        Err(DeleteAclsRequestFailure::EmptyPrincipal)
    );
    assert_eq!(
        delete_acls_request(&[filter(1, None, 1, None, Some(""), 1, 1)], usize::MAX,),
        Err(DeleteAclsRequestFailure::EmptyHost)
    );

    let oversized = "x".repeat(MAX_FILTER_STRING_BYTES + 1);
    assert_eq!(
        delete_acls_request(
            &[filter(1, Some(&oversized), 1, None, None, 1, 1)],
            usize::MAX,
        ),
        Err(DeleteAclsRequestFailure::ResourceNameTooLong {
            actual: MAX_FILTER_STRING_BYTES + 1,
            max: MAX_FILTER_STRING_BYTES,
        })
    );
}

#[test]
fn complete_generated_request_must_fit_the_retained_limit() {
    let filters = [
        any_filter(),
        filter(2, Some("orders"), 3, Some("User:a"), Some("*"), 3, 3),
    ];
    let required = request_peak_charge(&filters).expect("bounded charge");

    assert_eq!(
        delete_acls_request(&filters, required - 1),
        Err(DeleteAclsRequestFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    let request = delete_acls_request(&filters, required).expect("exact retained limit");
    assert!(request.retained_size().heap_bytes() <= required);
}

const fn any_filter() -> DeleteAclsFilterRef<'static> {
    filter(1, None, 1, None, None, 1, 1)
}

const fn filter<'a>(
    resource_type: i8,
    resource_name: Option<&'a str>,
    pattern_type: i8,
    principal: Option<&'a str>,
    host: Option<&'a str>,
    operation: i8,
    permission_type: i8,
) -> DeleteAclsFilterRef<'a> {
    DeleteAclsFilterRef::new(
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission_type,
    )
}
