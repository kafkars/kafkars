//! Generated `DescribeCluster` request and hostile-response scenarios.

use kafka_client_core::DescribeClusterInput;
use kafka_wire::{DescribeClusterResponse, describe_cluster_response::DescribeClusterBroker};

use super::describe_cluster::{
    DescribeClusterProtocolFailure, describe_cluster_request, normalize_describe_cluster_response,
};

const RESERVATION: usize = 128 * 1024;

#[test]
fn request_is_fixed_to_broker_endpoints_without_optional_expansion() {
    let request = describe_cluster_request(false, false);
    assert!(!request.include_cluster_authorized_operations);
    assert_eq!(request.endpoint_type, 1);
    assert!(!request.include_fenced_brokers);
}

#[test]
fn authorized_operations_request_and_response_preserve_exact_optional_bits() {
    let request = describe_cluster_request(false, true);
    assert!(request.include_cluster_authorized_operations);
    assert!(!request.include_fenced_brokers);

    let mut response = response();
    response.cluster_authorized_operations = 0x21;
    let normalized = normalize_describe_cluster_response(&response, RESERVATION, false, true);
    let Ok(DescribeClusterInput::BrokerResponded { description }) = normalized else {
        panic!("requested authorization bits must normalize");
    };
    assert_eq!(description.authorized_operations(), Some(0x21));

    response.cluster_authorized_operations = i32::MIN;
    let normalized = normalize_describe_cluster_response(&response, RESERVATION, false, true);
    let Ok(DescribeClusterInput::BrokerResponded { description }) = normalized else {
        panic!("requested absent authorization bits must normalize");
    };
    assert_eq!(description.authorized_operations(), None);
}

#[test]
fn unrequested_authorized_operations_are_a_response_mismatch() {
    let mut response = response();
    response.cluster_authorized_operations = 0x21;
    assert_eq!(
        normalize_describe_cluster_response(&response, RESERVATION, false, false),
        Err(DescribeClusterProtocolFailure::AuthorizedOperations)
    );
}

#[test]
fn explicit_fenced_view_is_encoded_and_preserved() {
    assert!(describe_cluster_request(true, false).include_fenced_brokers);
    let mut response = response();
    let mut fenced = broker(7, Some("rack-b"));
    fenced.is_fenced = true;
    response.brokers = vec![fenced];
    let normalized = normalize_describe_cluster_response(&response, RESERVATION, true, false);
    let Ok(DescribeClusterInput::BrokerResponded { description }) = normalized else {
        panic!("explicit fenced response must normalize");
    };
    assert!(description.brokers()[0].is_fenced());
}

#[test]
fn success_preserves_nullable_racks_and_orders_brokers_by_id() {
    let mut response = response();
    response.controller_id = 2;
    response.brokers = vec![broker(7, Some("rack-b")), broker(2, None)];
    let normalized = normalize_describe_cluster_response(&response, RESERVATION, false, false);
    let Ok(DescribeClusterInput::BrokerResponded { description }) = normalized else {
        panic!("valid response must normalize");
    };
    assert_eq!(description.cluster_id(), "cluster-a");
    assert_eq!(description.controller_id(), Some(2));
    assert_eq!(description.brokers()[0].id(), 2);
    assert_eq!(description.brokers()[0].rack(), None);
    assert_eq!(description.brokers()[1].rack(), Some("rack-b"));
}

#[test]
fn absent_controller_is_preserved_as_none() {
    let mut response = response();
    response.brokers = vec![broker(1, None)];
    let normalized = normalize_describe_cluster_response(&response, RESERVATION, false, false);
    let Ok(DescribeClusterInput::BrokerResponded { description }) = normalized else {
        panic!("valid response must normalize");
    };
    assert_eq!(description.controller_id(), None);
}

#[test]
fn negative_unknown_error_and_null_message_are_lossless() {
    let mut response = response();
    response.error_code = -32;
    response.error_message = None;
    let normalized = normalize_describe_cluster_response(&response, RESERVATION, false, false);
    let Ok(DescribeClusterInput::BrokerRejected { error }) = normalized else {
        panic!("broker error must normalize");
    };
    assert_eq!(error.into_parts(), (-32, None, false));
}

#[test]
fn excluded_fenced_brokers_and_empty_hosts_are_structural_failures() {
    let mut response = response();
    let mut fenced = broker(1, None);
    fenced.is_fenced = true;
    response.brokers = vec![fenced];
    assert_eq!(
        normalize_describe_cluster_response(&response, RESERVATION, false, false),
        Err(DescribeClusterProtocolFailure::FencedBroker)
    );

    let mut empty = broker(1, None);
    empty.host = "".into();
    response.brokers = vec![empty];
    assert_eq!(
        normalize_describe_cluster_response(&response, RESERVATION, false, false),
        Err(DescribeClusterProtocolFailure::EmptyHost)
    );
}

#[test]
fn controller_sentinels_are_nullable_but_other_negative_ids_are_rejected() {
    let mut response = response();
    response.controller_id = -2;
    assert_eq!(
        normalize_describe_cluster_response(&response, RESERVATION, false, false),
        Err(DescribeClusterProtocolFailure::ControllerId)
    );
}

#[test]
fn hostile_oversized_host_is_rejected_without_copying_it_into_the_error() {
    let hostile = "x".repeat(4 * 1024 * 1024);
    let mut response = response();
    let mut broker = broker(1, None);
    broker.host = hostile.as_str().into();
    response.brokers = vec![broker];
    assert_eq!(
        normalize_describe_cluster_response(&response, RESERVATION, false, false),
        Err(DescribeClusterProtocolFailure::HostBytes)
    );
}

#[test]
fn broker_count_and_terminal_bytes_are_bounded_before_copying() {
    let mut response = response();
    response.brokers = (0..257).map(|id| broker(id, None)).collect();
    assert_eq!(
        normalize_describe_cluster_response(&response, RESERVATION, false, false),
        Err(DescribeClusterProtocolFailure::BrokerCapacity)
    );
    response.brokers.truncate(2);
    assert_eq!(
        normalize_describe_cluster_response(&response, 1, false, false),
        Err(DescribeClusterProtocolFailure::RetainedBytes)
    );
}

fn response() -> DescribeClusterResponse {
    let mut response = DescribeClusterResponse::default();
    response.endpoint_type = 1;
    response.cluster_id = "cluster-a".into();
    response
}

fn broker(id: i32, rack: Option<&str>) -> DescribeClusterBroker {
    let mut broker = DescribeClusterBroker::default();
    broker.broker_id = id;
    broker.host = "broker.local".into();
    broker.port = 9092;
    broker.rack = rack.map(Into::into);
    broker
}
