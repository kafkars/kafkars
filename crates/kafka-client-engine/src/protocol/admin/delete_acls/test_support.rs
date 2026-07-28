//! Shared caller-prepared vectors and generated DeleteAcls test values.

use kafka_client_core::{DeleteAclFilterResult, DeleteAclMatchingBinding};
use kafka_wire::{
    DeleteAclsResponse,
    delete_acls_response::{
        DeleteAclsFilterResult as GeneratedFilterResult, DeleteAclsMatchingAcl,
    },
};
use kafka_wire_core::StrBytes;

use super::{
    DeleteAclsResponseFailure, NormalizedDeleteAclsResponse, normalize_delete_acls_response,
    retention::MAX_FILTERS,
};

pub(super) fn normalize(
    response: &DeleteAclsResponse,
    expected: usize,
    limit: usize,
) -> Result<NormalizedDeleteAclsResponse, DeleteAclsResponseFailure> {
    normalize_delete_acls_response(
        2,
        expected,
        response,
        limit,
        prepared_results(expected),
        |_filter_index, count| Ok(prepared_matching(count)),
    )
}

pub(super) fn normalize_versioned(
    version: i16,
    expected: usize,
    response: &DeleteAclsResponse,
) -> Result<NormalizedDeleteAclsResponse, DeleteAclsResponseFailure> {
    normalize_delete_acls_response(
        version,
        expected,
        response,
        usize::MAX,
        prepared_results(expected.min(MAX_FILTERS)),
        |_filter_index, count| Ok(prepared_matching(count)),
    )
}

pub(super) fn prepared_results(count: usize) -> Vec<DeleteAclFilterResult> {
    let mut results = Vec::new();
    results.try_reserve_exact(count).expect("outer capacity");
    results
}

pub(super) fn prepared_matching(count: usize) -> Vec<DeleteAclMatchingBinding> {
    let mut matching = Vec::new();
    matching.try_reserve_exact(count).expect("nested capacity");
    matching
}

pub(super) fn response(filter_results: Vec<GeneratedFilterResult>) -> DeleteAclsResponse {
    let mut response = DeleteAclsResponse::default();
    response.throttle_time_ms = 23;
    response.filter_results = filter_results;
    response
}

pub(super) fn filter_result(
    error_code: i16,
    error_message: Option<&str>,
    matching_acls: Vec<DeleteAclsMatchingAcl>,
) -> GeneratedFilterResult {
    let mut result = GeneratedFilterResult::default();
    result.error_code = error_code;
    result.error_message = error_message.map(StrBytes::from);
    result.matching_acls = matching_acls;
    result
}

pub(super) fn matching(
    error_code: i16,
    error_message: Option<&str>,
    resource_name: &str,
    principal: &str,
    host: &str,
    operation: i8,
) -> DeleteAclsMatchingAcl {
    let mut matching = DeleteAclsMatchingAcl::default();
    matching.error_code = error_code;
    matching.error_message = error_message.map(StrBytes::from);
    matching.resource_type = 2;
    matching.resource_name = resource_name.into();
    matching.pattern_type = 3;
    matching.principal = principal.into();
    matching.host = host.into();
    matching.operation = operation;
    matching.permission_type = 3;
    matching
}
