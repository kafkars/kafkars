//! Retained-result accounting scenarios for normalized topic descriptions.

use kafka_wire::{
    MetadataResponse,
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
};

use super::describe_topics_budget::ensure_result_fits;

#[test]
fn partition_and_replica_storage_must_fit_the_accepted_reservation() {
    let response = response();
    let topics = vec!["orders".to_owned()];

    assert!(!ensure_result_fits(&topics, &response, 1));
    assert!(ensure_result_fits(&topics, &response, 64 * 1024));
}

fn response() -> MetadataResponse {
    let mut partition = MetadataResponsePartition::default();
    partition.replica_nodes = vec![1, 2, 3];
    partition.isr_nodes = vec![1, 2];
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some("orders".into());
    topic.partitions = vec![partition];
    let mut response = MetadataResponse::default();
    response.topics = vec![topic];
    response
}
