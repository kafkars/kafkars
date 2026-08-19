//! Accounting evidence for ACL request, scratch, and normalized ownership.

use kafka_wire::{
    DescribeAclsResponse,
    describe_acls_response::{AclDescription, DescribeAclsResource},
};
use kafka_wire_core::StrBytes;

use super::{
    DescribeAclsFilterRef, normalize_describe_acls_response,
    retention::{MAX_DIAGNOSTIC_BYTES, request_retained_charge, response_peak_charge},
};

#[test]
fn request_charge_is_the_exact_optional_text_sum() {
    let filter =
        DescribeAclsFilterRef::new(1, Some("orders"), 3, Some("User:alice"), Some("*"), 2, 3);

    assert_eq!(
        request_retained_charge(filter),
        Some("orders".len() + "User:alice".len() + "*".len())
    );
    assert_eq!(
        request_retained_charge(DescribeAclsFilterRef::new(1, None, 3, None, None, 2, 3,)),
        Some(0)
    );
}

#[test]
fn diagnostic_charge_stops_at_one_utf8_safe_kibibyte() {
    let mut short = DescribeAclsResponse::default();
    short.error_code = -1;
    short.error_message = Some(StrBytes::from("x".repeat(MAX_DIAGNOSTIC_BYTES)));
    short.resources = Vec::new();
    let mut long = short.clone();
    long.error_message = Some(StrBytes::from(format!(
        "{}é",
        "x".repeat(MAX_DIAGNOSTIC_BYTES)
    )));

    assert_eq!(response_peak_charge(&short), response_peak_charge(&long));
}

#[test]
fn reported_peak_covers_materialized_normalized_ownership() {
    let mut acl = AclDescription::default();
    acl.principal = StrBytes::from("User:alice");
    acl.host = StrBytes::from("*");
    acl.operation = 3;
    acl.permission_type = 3;
    let mut resource = DescribeAclsResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from("orders");
    resource.pattern_type = 3;
    resource.acls = vec![acl];
    let mut response = DescribeAclsResponse::default();
    response.resources = vec![resource];

    let peak = response_peak_charge(&response).unwrap_or_else(|| panic!("bounded peak"));
    let normalized = normalize_describe_acls_response(3, &response, peak)
        .unwrap_or_else(|error| panic!("peak covers output: {error:?}"));
    assert_eq!(normalized.retained_bytes, peak);
}
