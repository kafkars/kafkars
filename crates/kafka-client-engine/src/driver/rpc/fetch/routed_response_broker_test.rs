//! Loopback Kafka broker for real routed driver-ownership scenarios.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    time::Duration,
};

use kafka_driver::ApiVersion;
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, FETCH_API_DESCRIPTOR, FetchRequest,
    FetchResponse, METADATA_API_DESCRIPTOR, MetadataRequest,
};

use crate::driver::DriverOwner;

use super::routed_response_broker_wire_test::{
    RequestFrame, api_versions_response, encode_response, metadata_response,
};

/// Opaque loopback peers kept alive for one routed-response scenario.
pub(crate) struct RoutedBroker {
    listener: TcpListener,
    port: u16,
    topic: String,
    partition_count: i32,
    all_partitions_available: bool,
    seed: Option<TcpStream>,
    long_poll: Option<TcpStream>,
}

impl RoutedBroker {
    pub(crate) fn new() -> Self {
        Self::with_topic_layout("events", 4, false)
    }

    pub(crate) fn with_available_topic(topic: &str, partition_count: usize) -> Self {
        let partition_count = i32::try_from(partition_count)
            .ok()
            .filter(|count| *count > 0)
            .unwrap_or_else(|| panic!("loopback topic partition count must fit positive i32"));
        Self::with_topic_layout(topic, partition_count, true)
    }

    fn with_topic_layout(
        topic: &str,
        partition_count: i32,
        all_partitions_available: bool,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|error| panic!("bind loopback Kafka broker: {error}"));
        let port = listener
            .local_addr()
            .unwrap_or_else(|error| panic!("read loopback broker address: {error}"))
            .port();
        Self {
            listener,
            port,
            topic: topic.to_owned(),
            partition_count,
            all_partitions_available,
            seed: None,
            long_poll: None,
        }
    }

    pub(crate) fn endpoint(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    pub(crate) fn await_seed(driver: &mut DriverOwner) {
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

    pub(crate) fn install_cluster(&mut self, driver: &mut DriverOwner) {
        let mut seed = accept_after_driving(&self.listener, driver);
        complete_negotiation(&mut seed, driver);
        respond_metadata(
            &mut seed,
            driver,
            self.port,
            false,
            &self.topic,
            self.partition_count,
            self.all_partitions_available,
        );
        self.seed = Some(seed);
    }

    pub(crate) fn install_topic(&mut self, driver: &mut DriverOwner) {
        let Some(seed) = self.seed.as_mut() else {
            panic!("cluster connection must precede topic Metadata");
        };
        respond_metadata(
            seed,
            driver,
            self.port,
            true,
            &self.topic,
            self.partition_count,
            self.all_partitions_available,
        );
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
    let response = api_versions_response();
    write_response(
        peer,
        &encode_response::<ApiVersionsRequest, _>(
            request.correlation_id,
            &response,
            ApiVersion::new(0),
        ),
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
    topic_name: &str,
    partition_count: i32,
    all_partitions_available: bool,
) {
    wait_for_frame(peer, driver, "write Metadata request");
    let request = read_request(peer);
    assert_eq!(request.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    let response = metadata_response(
        port,
        include_partition,
        topic_name,
        partition_count,
        all_partitions_available,
    );
    write_response(
        peer,
        &encode_response::<MetadataRequest, _>(
            request.correlation_id,
            &response,
            request.api_version,
        ),
    );
    drive(driver, Duration::from_secs(1), "install Metadata response");
}

fn respond_fetch(peer: &mut TcpStream, driver: &mut DriverOwner) -> (ApiVersion, FetchRequest) {
    wait_for_frame(peer, driver, "write Fetch request");
    let request = read_request(peer);
    assert_eq!(request.api_key, FETCH_API_DESCRIPTOR.api_key.value());
    assert!(matches!(request.api_version.value(), 12 | 16));
    let decoded = request.decode::<FetchRequest>();
    write_response(
        peer,
        &encode_response::<FetchRequest, _>(
            request.correlation_id,
            &FetchResponse::default(),
            request.api_version,
        ),
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
    RequestFrame::from_bytes(frame)
}

fn write_response(peer: &mut TcpStream, body: &[u8]) {
    let length =
        i32::try_from(body.len()).unwrap_or_else(|error| panic!("response length: {error}"));
    peer.write_all(&length.to_be_bytes())
        .and_then(|()| peer.write_all(body))
        .unwrap_or_else(|error| panic!("write response frame: {error}"));
}
