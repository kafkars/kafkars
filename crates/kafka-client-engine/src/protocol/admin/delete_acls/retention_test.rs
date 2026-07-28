//! Focused evidence for checked DeleteAcls byte accounting.

use core::mem::size_of;

use kafka_client_core::{DeleteAclFilterResult, DeleteAclMatchingBinding};
use kafka_wire::{
    DeleteAclsResponse,
    delete_acls_response::{
        DeleteAclsFilterResult as GeneratedFilterResult, DeleteAclsMatchingAcl,
    },
};
use kafka_wire_core::StrBytes;

use super::{
    DeleteAclsFilterRef,
    retention::{
        MAX_DIAGNOSTIC_BYTES, MatchingKey, bounded_diagnostic_len, request_peak_charge,
        response_peak_charge,
    },
};

#[test]
fn request_charge_counts_every_owner_and_present_filter_byte() {
    let filters = [
        DeleteAclsFilterRef::new(1, None, 1, None, None, 1, 1),
        DeleteAclsFilterRef::new(2, Some("orders"), 3, Some("User:a"), Some("*"), 3, 3),
    ];
    let owners = filters.len() * size_of::<kafka_wire::delete_acls_request::DeleteAclsFilter>();

    assert_eq!(
        request_peak_charge(&filters),
        Some(owners + "orders".len() + "User:a".len() + "*".len())
    );
}

#[test]
fn response_charge_includes_terminal_owners_text_and_largest_scratch() {
    let response = response(vec![
        filter_result(
            0,
            None,
            vec![
                matching(0, None, "orders", "User:a", "*"),
                matching(-7, Some("denied"), "audit", "User:b", "host"),
            ],
        ),
        filter_result(17, Some("filter"), Vec::new()),
    ]);
    let outer = 2 * size_of::<DeleteAclFilterResult>();
    let nested = 2 * size_of::<DeleteAclMatchingBinding>();
    let scratch = 2 * size_of::<MatchingKey<'static>>();
    let text = "orders".len()
        + "User:a".len()
        + "*".len()
        + "audit".len()
        + "User:b".len()
        + "host".len()
        + "denied".len()
        + "filter".len();

    assert_eq!(
        response_peak_charge(&response),
        Some(outer + nested + scratch + text)
    );
}

#[test]
fn diagnostic_charge_is_nullable_bounded_and_utf8_safe() {
    assert_eq!(bounded_diagnostic_len(None), 0);
    assert_eq!(bounded_diagnostic_len(Some("")), 0);
    let diagnostic = format!("{}é", "x".repeat(MAX_DIAGNOSTIC_BYTES - 1));
    assert_eq!(
        bounded_diagnostic_len(Some(&diagnostic)),
        MAX_DIAGNOSTIC_BYTES - 1
    );
}

fn response(filter_results: Vec<GeneratedFilterResult>) -> DeleteAclsResponse {
    let mut response = DeleteAclsResponse::default();
    response.filter_results = filter_results;
    response
}

fn filter_result(
    error_code: i16,
    message: Option<&str>,
    matching_acls: Vec<DeleteAclsMatchingAcl>,
) -> GeneratedFilterResult {
    let mut result = GeneratedFilterResult::default();
    result.error_code = error_code;
    result.error_message = message.map(StrBytes::from);
    result.matching_acls = matching_acls;
    result
}

fn matching(
    error_code: i16,
    message: Option<&str>,
    resource: &str,
    principal: &str,
    host: &str,
) -> DeleteAclsMatchingAcl {
    let mut matching = DeleteAclsMatchingAcl::default();
    matching.error_code = error_code;
    matching.error_message = message.map(StrBytes::from);
    matching.resource_type = 2;
    matching.resource_name = resource.into();
    matching.pattern_type = 3;
    matching.principal = principal.into();
    matching.host = host.into();
    matching.operation = 3;
    matching.permission_type = 3;
    matching
}
