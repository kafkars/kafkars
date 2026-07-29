//! Raw-terminal correlation retention for discovery and exact-broker attempts.

use kafka_client_core::AdminGroupListingFilters;
use kafka_driver::ApiVersion;
use kafka_wire::{DescribeClusterResponse, ListGroupsResponse};

use super::{
    retain_list_consumer_groups_broker_terminal, retain_list_consumer_groups_discovery_terminal,
};

#[test]
fn discovery_terminal_cannot_match_an_exact_broker_attempt() {
    let raw = retain_list_consumer_groups_discovery_terminal(
        Some(ApiVersion::new(2)),
        Ok(DescribeClusterResponse::default()),
        None,
    );

    assert!(raw.matches_discovery());
    assert!(!raw.matches_broker(7, &filters(), 4_096));
    raw.discard();
}

#[test]
fn broker_terminal_retains_exact_broker_filters_and_result_limit() {
    let expected = filters();
    let raw = retain_list_consumer_groups_broker_terminal(
        7,
        expected.clone(),
        4_096,
        Some(ApiVersion::new(5)),
        Ok(ListGroupsResponse::default()),
        None,
    );

    assert!(!raw.matches_discovery());
    assert!(raw.matches_broker(7, &expected, 4_096));
    assert!(!raw.matches_broker(8, &expected, 4_096));
    assert!(!raw.matches_broker(7, &AdminGroupListingFilters::empty(), 4_096));
    assert!(!raw.matches_broker(7, &expected, 4_095));
    raw.discard();
}

fn filters() -> AdminGroupListingFilters {
    AdminGroupListingFilters::new(
        vec!["Stable".to_owned()],
        vec!["consumer".to_owned()],
        vec!["consumer".to_owned()],
    )
    .unwrap_or_else(|error| panic!("valid filters: {error}"))
}
