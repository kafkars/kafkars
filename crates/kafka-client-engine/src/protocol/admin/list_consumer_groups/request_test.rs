//! Exact request filters, version floors, and retained limits.

use kafka_client_core::AdminGroupListingFilters;

use super::{ListConsumerGroupsRequestFailure, list_consumer_groups_request};

#[test]
fn empty_request_is_unfiltered_and_version_zero_compatible() {
    let (request, minimum_version) =
        list_consumer_groups_request(&AdminGroupListingFilters::empty(), 4096)
            .unwrap_or_else(|error| panic!("empty request: {error:?}"));
    assert!(request.states_filter.is_empty());
    assert!(request.types_filter.is_empty());
    assert_eq!(minimum_version, 0);
}

#[test]
fn state_and_type_filters_preserve_order_and_raise_exact_version_floor() {
    let filters = AdminGroupListingFilters::new(
        vec!["Stable".to_owned(), "Empty".to_owned()],
        vec!["share".to_owned(), "streams".to_owned()],
        vec!["consumer".to_owned()],
    )
    .unwrap_or_else(|error| panic!("valid filters: {error}"));
    let (request, minimum_version) = list_consumer_groups_request(&filters, 4096)
        .unwrap_or_else(|error| panic!("filtered request: {error:?}"));
    assert_eq!(
        request
            .states_filter
            .iter()
            .map(|value| value.as_str())
            .collect::<Vec<_>>(),
        vec!["Stable", "Empty"]
    );
    assert_eq!(
        request
            .types_filter
            .iter()
            .map(|value| value.as_str())
            .collect::<Vec<_>>(),
        vec!["share", "streams"]
    );
    assert_eq!(minimum_version, 5);
}

#[test]
fn bounded_materialization_rejects_insufficient_capacity() {
    let filters = AdminGroupListingFilters::new(vec!["Stable".to_owned()], Vec::new(), Vec::new())
        .unwrap_or_else(|error| panic!("valid filters: {error}"));
    assert_eq!(
        list_consumer_groups_request(&filters, 0),
        Err(ListConsumerGroupsRequestFailure::RetainedBytes)
    );
}
