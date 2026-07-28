//! Focused evidence for bounded caller-ordered ACL creation construction.

use kafka_wire::RetainedSize;

use super::{
    CreateAclBindingRef, CreateAclsRequestFailure, create_acls_request,
    retention::{MAX_BINDINGS, MAX_STRING_BYTES, request_peak_charge},
};

#[test]
fn request_preserves_caller_order_and_exact_binding_fields() {
    let bindings = [
        binding(2, "orders", 3, "User:alice", "*", 3, 3),
        binding(3, "payments", 4, "User:bob", "10.0.0.1", 8, 2),
    ];

    let request = create_acls_request(&bindings, usize::MAX).expect("valid batch");

    assert_eq!(request.creations.len(), 2);
    let first = &request.creations[0];
    assert_eq!(first.resource_type, 2);
    assert_eq!(first.resource_name.as_str(), "orders");
    assert_eq!(first.resource_pattern_type, 3);
    assert_eq!(first.principal.as_str(), "User:alice");
    assert_eq!(first.host.as_str(), "*");
    assert_eq!(first.operation, 3);
    assert_eq!(first.permission_type, 3);
    assert!(first.unknown_tagged_fields.is_empty());

    let second = &request.creations[1];
    assert_eq!(second.resource_type, 3);
    assert_eq!(second.resource_name.as_str(), "payments");
    assert_eq!(second.resource_pattern_type, 4);
    assert_eq!(second.principal.as_str(), "User:bob");
    assert_eq!(second.host.as_str(), "10.0.0.1");
    assert_eq!(second.operation, 8);
    assert_eq!(second.permission_type, 2);
    assert!(second.unknown_tagged_fields.is_empty());
    assert!(request.unknown_tagged_fields.is_empty());
}

#[test]
fn request_rejects_empty_oversized_and_duplicate_batches() {
    assert_eq!(
        create_acls_request(&[], usize::MAX),
        Err(CreateAclsRequestFailure::EmptyBatch)
    );

    let too_many = vec![valid_binding(); MAX_BINDINGS + 1];
    assert_eq!(
        create_acls_request(&too_many, usize::MAX),
        Err(CreateAclsRequestFailure::TooManyBindings {
            actual: MAX_BINDINGS + 1,
            max: MAX_BINDINGS,
        })
    );

    let duplicate = [valid_binding(), valid_binding()];
    assert_eq!(
        create_acls_request(&duplicate, usize::MAX),
        Err(CreateAclsRequestFailure::DuplicateBinding)
    );
}

#[test]
fn request_rejects_invalid_concrete_scalars_and_text() {
    assert_eq!(
        create_acls_request(&[binding(1, "orders", 3, "User:a", "*", 3, 3)], usize::MAX,),
        Err(CreateAclsRequestFailure::InvalidResourceType { actual: 1 })
    );
    assert_eq!(
        create_acls_request(&[binding(2, "orders", 2, "User:a", "*", 3, 3)], usize::MAX,),
        Err(CreateAclsRequestFailure::InvalidPatternType { actual: 2 })
    );
    assert_eq!(
        create_acls_request(&[binding(2, "orders", 3, "User:a", "*", 1, 3)], usize::MAX,),
        Err(CreateAclsRequestFailure::InvalidOperation { actual: 1 })
    );
    assert_eq!(
        create_acls_request(&[binding(2, "orders", 3, "User:a", "*", 3, 1)], usize::MAX,),
        Err(CreateAclsRequestFailure::InvalidPermissionType { actual: 1 })
    );

    assert_eq!(
        create_acls_request(&[binding(2, "", 3, "User:a", "*", 3, 3)], usize::MAX,),
        Err(CreateAclsRequestFailure::EmptyResourceName)
    );
    assert_eq!(
        create_acls_request(&[binding(2, "orders", 3, "", "*", 3, 3)], usize::MAX,),
        Err(CreateAclsRequestFailure::EmptyPrincipal)
    );
    assert_eq!(
        create_acls_request(&[binding(2, "orders", 3, "User:a", "", 3, 3)], usize::MAX,),
        Err(CreateAclsRequestFailure::EmptyHost)
    );

    let oversized = "x".repeat(MAX_STRING_BYTES + 1);
    assert_eq!(
        create_acls_request(
            &[binding(2, &oversized, 3, "User:a", "*", 3, 3)],
            usize::MAX,
        ),
        Err(CreateAclsRequestFailure::ResourceNameTooLong {
            actual: MAX_STRING_BYTES + 1,
            max: MAX_STRING_BYTES,
        })
    );
}

#[test]
fn complete_request_and_validation_peak_must_fit_before_allocation() {
    let bindings = [
        valid_binding(),
        binding(3, "audit", 4, "User:b", "10.0.0.1", 4, 2),
    ];
    let required = request_peak_charge(&bindings).expect("bounded charge");

    assert_eq!(
        create_acls_request(&bindings, required - 1),
        Err(CreateAclsRequestFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    let request = create_acls_request(&bindings, required).expect("exact peak");
    assert!(request.retained_size().heap_bytes() <= required);
}

const fn valid_binding() -> CreateAclBindingRef<'static> {
    binding(2, "orders", 3, "User:a", "*", 3, 3)
}

const fn binding<'a>(
    resource_type: i8,
    resource_name: &'a str,
    pattern_type: i8,
    principal: &'a str,
    host: &'a str,
    operation: i8,
    permission_type: i8,
) -> CreateAclBindingRef<'a> {
    CreateAclBindingRef::new(
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission_type,
    )
}
