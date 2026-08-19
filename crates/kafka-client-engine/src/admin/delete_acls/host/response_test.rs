//! Protocol-failure classification and positional nested-storage capacity evidence.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeleteAclBrokerError, DeleteAclFilterResult, DeleteAclMatchResult, DeleteAclMatchingBinding,
    DeleteAclsInput, DeliveryStatus,
};

use crate::protocol::admin::delete_acls::DeleteAclsResponseFailure;

use super::response::{protocol_failure, record_positional_matching_capacities};

#[test]
fn matching_capacities_preserve_every_filter_position_across_broker_failures() {
    let filter_results = [
        matched(2),
        broker_failed(-731),
        matched(0),
        broker_failed(-732),
    ];
    let mut capacities = Vec::new();
    capacities
        .try_reserve_exact(filter_results.len())
        .unwrap_or_else(|error| panic!("matching-capacity storage: {error:?}"));

    record_positional_matching_capacities(&filter_results, filter_results.len(), &mut capacities)
        .unwrap_or_else(|error| panic!("one prepared capacity per filter position: {error:?}"));

    assert_eq!(capacities, [2, 0, 0, 0]);
}

#[test]
fn compatibility_and_every_storage_failure_remain_distinct() {
    assert_eq!(
        protocol_failure(DeleteAclsResponseFailure::UnsupportedApiVersion { actual: 9 }),
        DeleteAclsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    for failure in [
        DeleteAclsResponseFailure::RetainedBytes {
            required: 11,
            limit: 10,
        },
        DeleteAclsResponseFailure::OuterResultStorage,
        DeleteAclsResponseFailure::MatchingResultStorage { filter_index: 2 },
        DeleteAclsResponseFailure::OwnedValueStorage,
    ] {
        assert_eq!(protocol_failure(failure), DeleteAclsInput::ResponseTooLarge);
    }
}

#[test]
fn malformed_positional_and_binding_shapes_are_invalid_responses() {
    for failure in [
        DeleteAclsResponseFailure::EmptyExpectedFilters,
        DeleteAclsResponseFailure::TooManyExpectedFilters { actual: 2, max: 1 },
        DeleteAclsResponseFailure::NegativeThrottleTime { actual: -1 },
        DeleteAclsResponseFailure::FilterResultCount {
            expected: 2,
            actual: 1,
        },
        DeleteAclsResponseFailure::FilterErrorWithMatches {
            filter_index: 0,
            actual: 1,
        },
        DeleteAclsResponseFailure::TooManyMatchesForFilter {
            filter_index: 0,
            actual: 2,
            max: 1,
        },
        DeleteAclsResponseFailure::TooManyMatchingAcls { actual: 2, max: 1 },
        DeleteAclsResponseFailure::InvalidResourceType { actual: 1 },
        DeleteAclsResponseFailure::EmptyResourceName,
        DeleteAclsResponseFailure::ResourceNameTooLong { actual: 2, max: 1 },
        DeleteAclsResponseFailure::InvalidPatternType { actual: 2 },
        DeleteAclsResponseFailure::EmptyPrincipal,
        DeleteAclsResponseFailure::PrincipalTooLong { actual: 2, max: 1 },
        DeleteAclsResponseFailure::EmptyHost,
        DeleteAclsResponseFailure::HostTooLong { actual: 2, max: 1 },
        DeleteAclsResponseFailure::InvalidOperation { actual: 1 },
        DeleteAclsResponseFailure::InvalidPermissionType { actual: 1 },
        DeleteAclsResponseFailure::DuplicateMatchingAcl { filter_index: 0 },
    ] {
        assert_eq!(protocol_failure(failure), DeleteAclsInput::InvalidResponse);
    }
}

fn matched(count: usize) -> DeleteAclFilterResult {
    DeleteAclFilterResult::Matched(vec![matching(); count])
}

fn broker_failed(code: i16) -> DeleteAclFilterResult {
    DeleteAclFilterResult::BrokerFailed(DeleteAclBrokerError::new(
        NonZeroI16::new(code).unwrap_or_else(|| panic!("nonzero broker code")),
        None,
        false,
    ))
}

fn matching() -> DeleteAclMatchingBinding {
    DeleteAclMatchingBinding::new(
        2,
        "orders".to_owned(),
        3,
        "User:alice".to_owned(),
        "*".to_owned(),
        3,
        3,
        DeleteAclMatchResult::Deleted,
    )
}
