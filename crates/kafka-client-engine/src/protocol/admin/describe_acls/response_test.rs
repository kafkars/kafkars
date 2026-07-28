//! Focused evidence for strict generated ACL response normalization.

use kafka_wire::{
    DescribeAclsResponse,
    describe_acls_response::{AclDescription, DescribeAclsResource},
};
use kafka_wire_core::StrBytes;

use super::{
    DescribeAclsResponseFailure, normalize_describe_acls_response,
    retention::{MAX_DIAGNOSTIC_BYTES, MAX_RESOURCE_NAME_BYTES, MAX_RESOURCES},
};

#[test]
fn response_flattens_and_orders_bindings_by_bytes_then_scalars() {
    let response = response(vec![
        resource(
            2,
            "orders",
            3,
            vec![acl("User:z", "*", 8, 3), acl("User:a", "10.0.0.1", -7, -8)],
        ),
        resource(-1, "audit", -2, vec![acl("User:m", "*", -3, -4)]),
    ]);

    let normalized =
        normalize_describe_acls_response(3, &response, usize::MAX).expect("valid response");
    assert_eq!(normalized.throttle_time_ms, 7);
    assert_eq!(normalized.error_code, 0);
    assert_eq!(normalized.bindings.len(), 3);

    let audit = &normalized.bindings[0];
    assert_eq!(audit.resource_name, "audit");
    assert_eq!(audit.principal, "User:m");
    assert_eq!(audit.host, "*");
    assert_eq!(
        (
            audit.resource_type,
            audit.pattern_type,
            audit.operation,
            audit.permission_type,
        ),
        (-1, -2, -3, -4)
    );

    assert_eq!(normalized.bindings[1].resource_name, "orders");
    assert_eq!(normalized.bindings[1].principal, "User:a");
    assert_eq!(normalized.bindings[2].principal, "User:z");
}

#[test]
fn response_preserves_signed_top_level_error_and_utf8_bounded_diagnostic() {
    let diagnostic = format!("{}é", "x".repeat(MAX_DIAGNOSTIC_BYTES - 1));
    let mut response = response(Vec::new());
    response.error_code = -42;
    response.error_message = Some(StrBytes::from(diagnostic.as_str()));

    let normalized = normalize_describe_acls_response(1, &response, usize::MAX)
        .expect("bounded broker rejection");
    assert_eq!(normalized.error_code, -42);
    assert_eq!(
        normalized.error_message.as_deref().map(str::len),
        Some(MAX_DIAGNOSTIC_BYTES - 1)
    );
    assert!(normalized.error_message_truncated);
    assert!(normalized.bindings.is_empty());
}

#[test]
fn response_rejects_versions_and_negative_throttle() {
    let response = response(Vec::new());
    assert_eq!(
        normalize_describe_acls_response(0, &response, usize::MAX),
        Err(DescribeAclsResponseFailure::UnsupportedApiVersion { actual: 0 })
    );
    assert_eq!(
        normalize_describe_acls_response(4, &response, usize::MAX),
        Err(DescribeAclsResponseFailure::UnsupportedApiVersion { actual: 4 })
    );

    let mut response = response;
    response.throttle_time_ms = -1;
    assert_eq!(
        normalize_describe_acls_response(2, &response, usize::MAX),
        Err(DescribeAclsResponseFailure::NegativeThrottleTime { actual: -1 })
    );
}

#[test]
fn response_rejects_resources_beside_top_level_error() {
    let mut response = response(vec![resource(
        2,
        "orders",
        3,
        vec![acl("User:a", "*", 3, 3)],
    )]);
    response.error_code = 31;

    assert_eq!(
        normalize_describe_acls_response(2, &response, usize::MAX),
        Err(DescribeAclsResponseFailure::ResourcesWithTopLevelError { actual: 1 })
    );
}

#[test]
fn response_rejects_duplicate_resource_and_acl_shapes() {
    let duplicate_resource = response(vec![
        resource(2, "orders", 3, vec![acl("User:a", "*", 3, 3)]),
        resource(2, "orders", 3, vec![acl("User:b", "*", 3, 3)]),
    ]);
    assert_eq!(
        normalize_describe_acls_response(2, &duplicate_resource, usize::MAX),
        Err(DescribeAclsResponseFailure::DuplicateResource)
    );

    let duplicate_acl = response(vec![resource(
        2,
        "orders",
        3,
        vec![acl("User:a", "*", 3, 3), acl("User:a", "*", 3, 3)],
    )]);
    assert_eq!(
        normalize_describe_acls_response(2, &duplicate_acl, usize::MAX),
        Err(DescribeAclsResponseFailure::DuplicateAcl)
    );
}

#[test]
fn response_rejects_empty_and_oversized_text_shapes() {
    let empty_resource = response(vec![resource(2, "", 3, vec![acl("User:a", "*", 3, 3)])]);
    assert_eq!(
        normalize_describe_acls_response(2, &empty_resource, usize::MAX),
        Err(DescribeAclsResponseFailure::EmptyResourceName)
    );

    let empty_acls = response(vec![resource(2, "orders", 3, Vec::new())]);
    assert_eq!(
        normalize_describe_acls_response(2, &empty_acls, usize::MAX),
        Err(DescribeAclsResponseFailure::EmptyResourceAcls)
    );

    let empty_principal = response(vec![resource(2, "orders", 3, vec![acl("", "*", 3, 3)])]);
    assert_eq!(
        normalize_describe_acls_response(2, &empty_principal, usize::MAX),
        Err(DescribeAclsResponseFailure::EmptyPrincipal)
    );

    let empty_host = response(vec![resource(
        2,
        "orders",
        3,
        vec![acl("User:a", "", 3, 3)],
    )]);
    assert_eq!(
        normalize_describe_acls_response(2, &empty_host, usize::MAX),
        Err(DescribeAclsResponseFailure::EmptyHost)
    );

    let oversized = "x".repeat(MAX_RESOURCE_NAME_BYTES + 1);
    let oversized_resource = response(vec![resource(
        2,
        &oversized,
        3,
        vec![acl("User:a", "*", 3, 3)],
    )]);
    assert_eq!(
        normalize_describe_acls_response(2, &oversized_resource, usize::MAX),
        Err(DescribeAclsResponseFailure::ResourceNameTooLong {
            actual: MAX_RESOURCE_NAME_BYTES + 1,
            max: MAX_RESOURCE_NAME_BYTES,
        })
    );
}

#[test]
fn response_rejects_hostile_resource_count_before_entry_validation() {
    let response = response(vec![DescribeAclsResource::default(); MAX_RESOURCES + 1]);

    assert_eq!(
        normalize_describe_acls_response(2, &response, usize::MAX),
        Err(DescribeAclsResponseFailure::TooManyResources {
            actual: MAX_RESOURCES + 1,
            max: MAX_RESOURCES,
        })
    );
}

#[test]
fn response_checks_peak_scratch_and_output_capacity_before_copying() {
    let response = response(vec![resource(
        2,
        "orders",
        3,
        vec![acl("User:a", "*", 3, 3)],
    )]);
    let normalized =
        normalize_describe_acls_response(2, &response, usize::MAX).expect("measure peak");
    let required = normalized.retained_bytes;

    assert_eq!(
        normalize_describe_acls_response(2, &response, required - 1),
        Err(DescribeAclsResponseFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    assert!(normalize_describe_acls_response(2, &response, required).is_ok());
}

fn response(resources: Vec<DescribeAclsResource>) -> DescribeAclsResponse {
    let mut response = DescribeAclsResponse::default();
    response.throttle_time_ms = 7;
    response.error_code = 0;
    response.error_message = None;
    response.resources = resources;
    response
}

fn resource(
    resource_type: i8,
    resource_name: &str,
    pattern_type: i8,
    acls: Vec<AclDescription>,
) -> DescribeAclsResource {
    let mut resource = DescribeAclsResource::default();
    resource.resource_type = resource_type;
    resource.resource_name = StrBytes::from(resource_name);
    resource.pattern_type = pattern_type;
    resource.acls = acls;
    resource
}

fn acl(principal: &str, host: &str, operation: i8, permission_type: i8) -> AclDescription {
    let mut acl = AclDescription::default();
    acl.principal = StrBytes::from(principal);
    acl.host = StrBytes::from(host);
    acl.operation = operation;
    acl.permission_type = permission_type;
    acl
}
