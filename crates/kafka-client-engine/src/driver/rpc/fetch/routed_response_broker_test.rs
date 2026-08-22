//! Loopback Kafka framing for real routed Fetch-token ownership scenarios.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use kafka_driver::ApiVersion;
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, FETCH_API_DESCRIPTOR,
    FetchRequest, FetchResponse, METADATA_API_DESCRIPTOR, MetadataRequest, MetadataResponse,
    RequestResponsePair, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    response_header_version_for,
};
use kafka_wire_core::{KafkaEncode, StrBytes, Uuid};

use crate::driver::DriverOwner;

/// Opaque loopback peers kept alive for one routed-response scenario.
pub(in crate::driver::rpc) struct RoutedBroker {
    listener: TcpListener,
    port: u16,
    seed: Option<TcpStream>,
    long_poll: Option<TcpStream>,
}

impl RoutedBroker {
    pub(in crate::driver::rpc) fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|error| panic!("bind loopback Kafka broker: {error}"));
        let port = listener
            .local_addr()
            .unwrap_or_else(|error| panic!("read loopback broker address: {error}"))
            .port();
        Self {
            listener,
            port,
            seed: None,
            long_poll: None,
        }
    }

    pub(in crate::driver::rpc) fn endpoint(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    pub(in crate::driver::rpc) fn await_seed(driver: &mut DriverOwner) {
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
        panic!("driver did not install the expected bootstrap seed")
    }

    pub(in crate::driver::rpc) fn install_cluster(&mut self, driver: &mut DriverOwner) {
        let mut seed = accept_after_driving(&self.listener, driver);
        complete_negotiation(&mut seed, driver);
        respond_metadata(&mut seed, driver, self.port, false);
        self.seed = Some(seed);
    }

    pub(in crate::driver::rpc) fn install_topic(&mut self, driver: &mut DriverOwner) {
        let Some(seed) = self.seed.as_mut() else {
            panic!("cluster connection must precede topic Metadata");
        };
        respond_metadata(seed, driver, self.port, true);
    }

    pub(super) fn complete_fetch(&mut self, driver: &mut DriverOwner) -> ApiVersion {
        self.complete_fetch_request(driver).0
    }

    pub(super) fn complete_fetch_request(
        &mut self,
        driver: &mut DriverOwner,
    ) -> (ApiVersion, FetchRequest) {
        let mut long_poll = accept_after_driving(&self.listener, driver);
        complete_negotiation(&mut long_poll, driver);
        let completed = respond_fetch(&mut long_poll, driver);
        self.long_poll = Some(long_poll);
        completed
    }
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
    panic!("driver did not open the expected broker connection")
}

fn complete_negotiation(peer: &mut TcpStream, driver: &mut DriverOwner) {
    wait_for_frame(peer, driver, "write ApiVersions request");
    let request = read_request(peer);
    assert_eq!(request.api_key, API_VERSIONS_API_DESCRIPTOR.api_key.value());
    let mut response = ApiVersionsResponse::default();
    response.api_keys = vec![
        advertisement(API_VERSIONS_API_DESCRIPTOR.api_key.value(), 0, 0),
        advertisement(METADATA_API_DESCRIPTOR.api_key.value(), 0, 13),
        advertisement(FETCH_API_DESCRIPTOR.api_key.value(), 4, 16),
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

fn respond_metadata(
    peer: &mut TcpStream,
    driver: &mut DriverOwner,
    port: u16,
    include_partition: bool,
) {
    wait_for_frame(peer, driver, "write Metadata request");
    let request = read_request(peer);
    assert_eq!(request.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    let mut response = MetadataResponse::default();
    response.brokers.push(broker(port));
    response.controller_id = 1;
    if include_partition {
        let mut topic = MetadataResponseTopic::default();
        topic.name = Some(StrBytes::from("events"));
        topic.topic_id = Uuid::from_bytes([7; 16]);
        for partition_index in 0..=3 {
            let mut partition = MetadataResponsePartition::default();
            partition.partition_index = partition_index;
            partition.leader_id = if partition_index == 3 { 1 } else { -1 };
            partition.leader_epoch = if partition_index == 3 { 9 } else { -1 };
            topic.partitions.push(partition);
        }
        response.topics.push(topic);
    }
    write_response::<MetadataRequest, _>(
        peer,
        request.correlation_id,
        &response,
        request.api_version,
    );
    drive(driver, Duration::from_secs(1), "install Metadata response");
}

fn respond_fetch(peer: &mut TcpStream, driver: &mut DriverOwner) -> (ApiVersion, FetchRequest) {
    wait_for_frame(peer, driver, "write Fetch request");
    let request = read_request(peer);
    assert_eq!(request.api_key, FETCH_API_DESCRIPTOR.api_key.value());
    assert!(matches!(request.api_version.value(), 12 | 16));
    let decoded = request.decode::<FetchRequest>();
    write_response::<FetchRequest, _>(
        peer,
        request.correlation_id,
        &FetchResponse::default(),
        request.api_version,
    );
    drive(driver, Duration::from_secs(1), "install Fetch response");
    (request.api_version, decoded)
}

pub(super) fn drive(driver: &mut DriverOwner, wait: Duration, phase: &str) {
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
    panic!("{phase} did not produce a broker frame")
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
        bytes: Bytes::from(frame),
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

fn broker(port: u16) -> MetadataResponseBroker {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 1;
    broker.host = StrBytes::from("127.0.0.1");
    broker.port = i32::from(port);
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

pub(super) struct RequestFrame {
    pub(super) api_key: i16,
    pub(super) api_version: ApiVersion,
    pub(super) correlation_id: i32,
    pub(super) bytes: Bytes,
}
