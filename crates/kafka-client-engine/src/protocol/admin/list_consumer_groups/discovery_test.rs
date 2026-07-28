//! Controller-discovery correlation and boundedness scenarios.

use kafka_wire::{DescribeClusterResponse, describe_cluster_response::DescribeClusterBroker};
use kafka_wire_core::StrBytes;

use super::{NormalizedListConsumerGroupsDiscovery, normalize_list_consumer_groups_discovery};

#[test]
fn discovery_extracts_sorted_validated_broker_ids() {
    let mut response = DescribeClusterResponse::default();
    response.cluster_id = StrBytes::from("cluster");
    response.endpoint_type = 1;
    response.controller_id = 8;
    response.brokers = vec![broker(8, "b"), broker(3, "a")];
    let normalized = normalize_list_consumer_groups_discovery(Some(2), &response, 4 * 1024 * 1024)
        .unwrap_or_else(|error| panic!("discovery: {error:?}"));
    let NormalizedListConsumerGroupsDiscovery::Brokers { broker_ids, .. } = normalized else {
        panic!("brokers");
    };
    assert_eq!(broker_ids, vec![3, 8]);
}

fn broker(id: i32, host: &str) -> DescribeClusterBroker {
    let mut broker = DescribeClusterBroker::default();
    broker.broker_id = id;
    broker.host = StrBytes::from(host);
    broker.port = 9092;
    broker
}
