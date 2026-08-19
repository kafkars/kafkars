//! Accounting evidence for creation request scratch and terminal result ownership.

use kafka_wire::{CreateAclsResponse, RetainedSize, create_acls_response::AclCreationResult};
use kafka_wire_core::StrBytes;

use super::{
    CreateAclBindingRef, create_acls_request, normalize_create_acls_response,
    retention::{MAX_DIAGNOSTIC_BYTES, request_peak_charge, response_peak_charge},
};

#[test]
fn request_peak_covers_generated_ownership_and_duplicate_scratch() {
    let bindings = [
        CreateAclBindingRef::new(2, "orders", 3, "User:a", "*", 3, 3),
        CreateAclBindingRef::new(3, "audit", 4, "User:b", "10.0.0.1", 4, 2),
    ];
    let peak = request_peak_charge(&bindings).unwrap_or_else(|| panic!("bounded peak"));
    let request = create_acls_request(&bindings, peak)
        .unwrap_or_else(|error| panic!("peak covers request: {error:?}"));

    assert!(peak > request.retained_size().heap_bytes());
}

#[test]
fn diagnostic_charge_stops_at_one_utf8_safe_kibibyte_per_result() {
    let short = response_with_message("x".repeat(MAX_DIAGNOSTIC_BYTES));
    let long = response_with_message(format!("{}é", "x".repeat(MAX_DIAGNOSTIC_BYTES)));

    assert_eq!(response_peak_charge(&short), response_peak_charge(&long));
}

#[test]
fn reported_charge_is_only_bounded_diagnostics_beyond_caller_storage() {
    let response = response_with_message("broker rejected binding".to_owned());
    let peak = response_peak_charge(&response).unwrap_or_else(|| panic!("bounded peak"));
    let normalized = normalize_create_acls_response(3, 1, &response, peak, |_| Ok(()))
        .unwrap_or_else(|error| panic!("charge covers borrowed visit: {error:?}"));

    assert_eq!(normalized.1, peak);
    assert_eq!(peak, "broker rejected binding".len());
}

#[test]
fn no_diagnostics_require_no_protocol_owned_response_capacity() {
    let mut result = AclCreationResult::default();
    result.error_message = None;
    let mut response = CreateAclsResponse::default();
    response.results = vec![result];

    assert_eq!(response_peak_charge(&response), Some(0));
    assert_eq!(
        normalize_create_acls_response(3, 1, &response, 0, |_| Ok(())),
        Ok((0, 0))
    );
}

fn response_with_message(message: String) -> CreateAclsResponse {
    let mut result = AclCreationResult::default();
    result.error_code = -1;
    result.error_message = Some(StrBytes::from(message));
    let mut response = CreateAclsResponse::default();
    response.results = vec![result];
    response
}
