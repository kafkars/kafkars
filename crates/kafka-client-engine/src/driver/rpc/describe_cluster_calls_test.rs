//! Bounded plain-call ownership scenarios for `DescribeCluster`.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};

use bytes::BytesMut;
use kafka_client_core::{Deadline, DescribeClusterInput, OperationId};
use kafka_driver::ApiVersion;
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse,
    DESCRIBE_CLUSTER_API_DESCRIPTOR, DescribeClusterRequest, DescribeClusterResponse,
    RequestResponsePair, ResponseHeader, api_versions_response::ApiVersion as AdvertisedApi,
    describe_cluster_response::DescribeClusterBroker, response_header_version_for,
};
use kafka_wire_core::{KafkaEncode, StrBytes};

use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use super::describe_cluster_calls::DescribeClusterCalls;

#[test]
fn call_capacity_is_explicit_and_non_growing() {
    let mut calls = DescribeClusterCalls::new(1);
    assert!(calls.try_reserve().is_some());
    assert_eq!(calls.retained_count(), 0);
    assert!(calls.try_reserve().is_some());
}

#[test]
fn generated_request_and_response_complete_through_a_loopback_broker() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback Kafka broker: {error}"));
    let endpoint = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"))
        .to_string();
    let mut driver = DriverOwner::build(&EngineConfig::new(vec![endpoint]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    await_seed(&mut driver);
    let mut peer = accept_after_driving(&listener, &mut driver);
    complete_negotiation(&mut peer, &mut driver);

    let mut calls = DescribeClusterCalls::new(1);
    calls
        .try_reserve()
        .unwrap_or_else(|| panic!("DescribeCluster capacity must be available"))
        .submit(
            &driver,
            OperationId::from_raw(7),
            OperationDeadline::from_parts_for_test(
                Deadline::from_tick(5_000_000_000),
                Instant::now() + Duration::from_secs(60),
            ),
            64 * 1024,
            false,
            false,
        )
        .unwrap_or_else(|_error| panic!("DescribeCluster call must be accepted"));

    wait_for_frame(&peer, &mut driver, "write DescribeCluster request");
    let request = read_request(&mut peer);
    assert_eq!(
        request.api_key,
        DESCRIBE_CLUSTER_API_DESCRIPTOR.api_key.value()
    );
    assert_eq!(request.api_version, ApiVersion::new(2));

    let mut response = DescribeClusterResponse::default();
    response.cluster_id = StrBytes::from("beta-cluster");
    response.controller_id = 2;
    response.brokers = vec![
        broker(9, "broker-nine", 9_099, Some("rack-b")),
        broker(2, "broker-two", 9_092, None),
    ];
    write_response::<DescribeClusterRequest, _>(
        &mut peer,
        request.correlation_id,
        &response,
        request.api_version,
    );
    let mut turns = 0;
    let settled = loop {
        assert!(turns < 32, "DescribeCluster result must become ready");
        turns += 1;
        drive(
            &mut driver,
            Duration::from_millis(100),
            "install DescribeCluster response",
        );
        if let Some(settled) = calls
            .poll_next_ready()
            .unwrap_or_else(|error| panic!("poll DescribeCluster result: {error}"))
        {
            break settled;
        }
    };
    assert_eq!(settled.operation_id(), OperationId::from_raw(7));
    let Some(DescribeClusterInput::BrokerResponded { description }) = settled.take_input() else {
        panic!("loopback response must normalize into a successful cluster description");
    };
    assert_eq!(description.cluster_id(), "beta-cluster");
    assert_eq!(description.controller_id(), Some(2));
    let broker_ids = description
        .brokers()
        .iter()
        .map(kafka_client_core::ClusterBroker::id)
        .collect::<Vec<_>>();
    assert_eq!(broker_ids, [2, 9]);
    assert_eq!(description.brokers()[1].rack(), Some("rack-b"));
    assert_eq!(calls.retained_count(), 1);
    calls.discard_settled();
    assert_eq!(calls.retained_count(), 0);
}

fn await_seed(driver: &mut DriverOwner) {
    for _turn in 0..32 {
        drive(driver, Duration::from_millis(100), "resolve bootstrap seed");
        let snapshot = driver
            .driver
            .snapshot()
            .unwrap_or_else(|error| panic!("request bootstrap snapshot: {error}"));
        drive(driver, Duration::ZERO, "capture bootstrap snapshot");
        match snapshot.try_result() {
            Some(Ok(Ok(snapshot))) if snapshot.seed().is_some() => return,
            Some(Ok(Ok(_snapshot))) => {}
            Some(Ok(Err(error))) => panic!("build bootstrap snapshot: {error}"),
            Some(Err(error)) => panic!("observe bootstrap snapshot: {error}"),
            None => {}
        }
    }
    panic!("driver did not install the expected bootstrap seed");
}

fn accept_after_driving(listener: &TcpListener, driver: &mut DriverOwner) -> TcpStream {
    listener
        .set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make broker listener nonblocking: {error}"));
    for _turn in 0..32 {
        drive(driver, Duration::from_millis(100), "open broker connection");
        match listener.accept() {
            Ok((peer, _address)) => {
                peer.set_nonblocking(false)
                    .unwrap_or_else(|error| panic!("make broker peer blocking: {error}"));
                return peer;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("accept broker connection: {error}"),
        }
    }
    panic!("driver did not open the expected broker connection");
}

fn complete_negotiation(peer: &mut TcpStream, driver: &mut DriverOwner) {
    wait_for_frame(peer, driver, "write ApiVersions request");
    let request = read_request(peer);
    assert_eq!(request.api_key, API_VERSIONS_API_DESCRIPTOR.api_key.value());
    let mut response = ApiVersionsResponse::default();
    response.api_keys = vec![
        advertisement(API_VERSIONS_API_DESCRIPTOR.api_key.value(), 0, 0),
        advertisement(DESCRIBE_CLUSTER_API_DESCRIPTOR.api_key.value(), 0, 2),
    ];
    write_response::<ApiVersionsRequest, _>(
        peer,
        request.correlation_id,
        &response,
        ApiVersion::new(0),
    );
    drive(
        driver,
        Duration::from_secs(1),
        "install ApiVersions response",
    );
}

fn drive(driver: &mut DriverOwner, wait: Duration, phase: &str) {
    driver
        .turn(wait)
        .unwrap_or_else(|error| panic!("{phase}: {error}"));
}

fn wait_for_frame(peer: &TcpStream, driver: &mut DriverOwner, phase: &str) {
    peer.set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make broker peer nonblocking: {error}"));
    let mut byte = [0; 1];
    for _turn in 0..32 {
        drive(driver, Duration::from_millis(100), phase);
        match peer.peek(&mut byte) {
            Ok(observed) if observed != 0 => {
                peer.set_nonblocking(false)
                    .unwrap_or_else(|error| panic!("make broker peer blocking: {error}"));
                return;
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("inspect broker request: {error}"),
        }
    }
    panic!("{phase} did not produce a broker frame");
}

fn read_request(peer: &mut TcpStream) -> RequestFrame {
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound broker read: {error}"));
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("validate frame length: {error}"));
    let mut frame = vec![0; length];
    peer.read_exact(&mut frame)
        .unwrap_or_else(|error| panic!("read request frame: {error}"));
    RequestFrame {
        api_key: read_i16(&frame, 0),
        api_version: ApiVersion::new(read_i16(&frame, 2)),
        correlation_id: read_i32(&frame, 4),
    }
}

fn write_response<R, T>(
    peer: &mut TcpStream,
    correlation_id: i32,
    response: &T,
    version: ApiVersion,
) where
    R: RequestResponsePair<Response = T>,
    T: KafkaEncode,
{
    let header_version = response_header_version_for::<R>(version)
        .unwrap_or_else(|error| panic!("response header policy: {error}"));
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;
    header
        .encode_into(&mut body, ApiVersion::new(header_version))
        .unwrap_or_else(|error| panic!("encode response header: {error}"));
    response
        .encode_into(&mut body, version)
        .unwrap_or_else(|error| panic!("encode response body: {error}"));
    let length =
        i32::try_from(body.len()).unwrap_or_else(|error| panic!("response length: {error}"));
    peer.write_all(&length.to_be_bytes())
        .and_then(|()| peer.write_all(&body))
        .unwrap_or_else(|error| panic!("write response frame: {error}"));
}

fn advertisement(api_key: i16, min_version: i16, max_version: i16) -> AdvertisedApi {
    let mut api = AdvertisedApi::default();
    api.api_key = api_key;
    api.min_version = min_version;
    api.max_version = max_version;
    api
}

fn broker(
    broker_id: i32,
    host: &'static str,
    port: i32,
    rack: Option<&'static str>,
) -> DescribeClusterBroker {
    let mut broker = DescribeClusterBroker::default();
    broker.broker_id = broker_id;
    broker.host = StrBytes::from(host);
    broker.port = port;
    broker.rack = rack.map(StrBytes::from);
    broker
}

fn read_i16(bytes: &[u8], offset: usize) -> i16 {
    let encoded = bytes
        .get(offset..offset + 2)
        .and_then(|bytes| bytes.try_into().ok())
        .unwrap_or_else(|| panic!("request must contain i16 at {offset}"));
    i16::from_be_bytes(encoded)
}

fn read_i32(bytes: &[u8], offset: usize) -> i32 {
    let encoded = bytes
        .get(offset..offset + 4)
        .and_then(|bytes| bytes.try_into().ok())
        .unwrap_or_else(|| panic!("request must contain i32 at {offset}"));
    i32::from_be_bytes(encoded)
}

struct RequestFrame {
    api_key: i16,
    api_version: ApiVersion,
    correlation_id: i32,
}
