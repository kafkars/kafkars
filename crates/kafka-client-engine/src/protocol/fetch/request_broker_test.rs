//! Broker-aggregated Fetch and explicit session-control request scenarios.

use kafka_wire_core::{ApiVersion, KafkaEncode};

use super::{
    BrokerFetchPartition, FETCH_TOPIC_ID_ROUTE_VERSION, FetchRequestSettings, FetchSessionRequest,
    ForgottenFetchPartition, broker_fetch_request, fetch_session_close_request,
};

fn settings() -> FetchRequestSettings {
    FetchRequestSettings::new(500, 1, 50 * 1024 * 1024, 1024 * 1024, 0)
}

#[test]
fn one_broker_request_aggregates_topics_and_partitions_in_input_order() {
    let request = broker_fetch_request(
        &[
            BrokerFetchPartition::new("alpha", [1; 16], Some(7), 1, 11),
            BrokerFetchPartition::new("beta", [2; 16], Some(8), 2, 22),
            BrokerFetchPartition::new("alpha", [1; 16], Some(7), 3, 33),
        ],
        &[],
        settings(),
        FetchSessionRequest::INITIAL,
    )
    .unwrap_or_else(|error| panic!("broker Fetch request: {error:?}"));

    assert_eq!(request.topics.len(), 2);
    assert_eq!(request.topics[0].topic.as_str(), "alpha");
    assert_eq!(request.topics[0].topic_id.to_bytes(), [1; 16]);
    assert_eq!(
        request.topics[0]
            .partitions
            .iter()
            .map(|partition| (partition.partition, partition.fetch_offset))
            .collect::<Vec<_>>(),
        vec![(1, 11), (3, 33)]
    );
    assert_eq!(request.topics[1].topic.as_str(), "beta");
    assert_eq!(request.topics[1].topic_id.to_bytes(), [2; 16]);
    assert_eq!(request.topics[1].partitions[0].partition, 2);
    assert_eq!(request.topics[1].partitions[0].current_leader_epoch, 8);
    assert_eq!((request.session_id, request.session_epoch), (0, 0));
    assert!(
        request
            .encoded_len(ApiVersion::new(FETCH_TOPIC_ID_ROUTE_VERSION))
            .is_ok()
    );
}

#[test]
fn incremental_request_groups_forgotten_partitions_by_topic() {
    let session = FetchSessionRequest::incremental(91, 4)
        .unwrap_or_else(|| panic!("positive incremental session"));
    let request = broker_fetch_request(
        &[BrokerFetchPartition::new("active", [3; 16], Some(9), 0, 10)],
        &[
            ForgottenFetchPartition::new("alpha", [1; 16], 1),
            ForgottenFetchPartition::new("beta", [2; 16], 2),
            ForgottenFetchPartition::new("alpha", [1; 16], 3),
        ],
        settings(),
        session,
    )
    .unwrap_or_else(|error| panic!("incremental Fetch request: {error:?}"));

    assert_eq!((request.session_id, request.session_epoch), (91, 4));
    assert_eq!(request.forgotten_topics_data.len(), 2);
    assert_eq!(request.forgotten_topics_data[0].topic.as_str(), "alpha");
    assert_eq!(
        request.forgotten_topics_data[0].topic_id.to_bytes(),
        [1; 16]
    );
    assert_eq!(request.forgotten_topics_data[0].partitions, vec![1, 3]);
    assert_eq!(request.forgotten_topics_data[1].topic.as_str(), "beta");
    assert_eq!(request.forgotten_topics_data[1].partitions, vec![2]);
    assert!(
        request
            .encoded_len(ApiVersion::new(FETCH_TOPIC_ID_ROUTE_VERSION))
            .is_ok()
    );
}

#[test]
fn final_epoch_close_has_no_partition_payload_and_requires_a_live_session() {
    assert!(fetch_session_close_request(settings(), FetchSessionRequest::LEGACY).is_none());
    assert!(fetch_session_close_request(settings(), FetchSessionRequest::INITIAL).is_none());
    let live = FetchSessionRequest::incremental(91, 7)
        .unwrap_or_else(|| panic!("positive incremental session"));
    let close = fetch_session_close_request(settings(), live)
        .unwrap_or_else(|| panic!("live session close"))
        .unwrap_or_else(|error| panic!("close request: {error:?}"));

    assert_eq!((close.session_id, close.session_epoch), (91, -1));
    assert!(close.topics.is_empty());
    assert!(close.forgotten_topics_data.is_empty());
    assert!(
        close
            .encoded_len(ApiVersion::new(FETCH_TOPIC_ID_ROUTE_VERSION))
            .is_ok()
    );

    let already_final = live
        .close()
        .unwrap_or_else(|| panic!("live session has final epoch"));
    let close = fetch_session_close_request(settings(), already_final)
        .unwrap_or_else(|| panic!("final session remains a close"))
        .unwrap_or_else(|error| panic!("final close request: {error:?}"));
    assert_eq!((close.session_id, close.session_epoch), (91, -1));
}
