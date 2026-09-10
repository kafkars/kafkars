//! Sole socket and Kafka-frame owner for the `CreateTopics` loopback scenario.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::ApiVersion;
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse,
    CREATE_TOPICS_API_DESCRIPTOR, METADATA_API_DESCRIPTOR, RequestResponsePair, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi, response_header_version_for,
};
use kafka_wire_core::KafkaEncode;

use crate::driver::DriverOwner;

pub(super) struct LoopbackListener(TcpListener);

impl LoopbackListener {
    pub(super) fn bind() -> Self {
        Self(
            TcpListener::bind("127.0.0.1:0")
                .unwrap_or_else(|error| panic!("bind loopback Kafka broker: {error}")),
        )
    }

    pub(super) fn address(&self) -> String {
        self.0
            .local_addr()
            .unwrap_or_else(|error| panic!("read loopback broker address: {error}"))
            .to_string()
    }

    pub(super) fn port(&self) -> u16 {
        self.0
            .local_addr()
            .unwrap_or_else(|error| panic!("read loopback broker address: {error}"))
            .port()
    }

    pub(super) fn await_seed(driver: &mut DriverOwner) {
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

    pub(super) fn accept(&self, driver: &mut DriverOwner) -> LoopbackConnection {
        self.0
            .set_nonblocking(true)
            .unwrap_or_else(|error| panic!("make broker listener nonblocking: {error}"));
        for _turn in 0..32 {
            drive(driver, Duration::from_millis(100), "open broker connection");
            match self.0.accept() {
                Ok((peer, _address)) => {
                    peer.set_nonblocking(false)
                        .unwrap_or_else(|error| panic!("make broker peer blocking: {error}"));
                    return LoopbackConnection(peer);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("accept broker connection: {error}"),
            }
        }
        panic!("driver did not open the expected broker connection")
    }
}

pub(super) struct LoopbackConnection(TcpStream);

impl LoopbackConnection {
    pub(super) fn negotiate(&mut self, driver: &mut DriverOwner) {
        let request = self.read_after_turns(driver, "write ApiVersions request");
        assert_eq!(request.api_key, API_VERSIONS_API_DESCRIPTOR.api_key.value());
        let mut response = ApiVersionsResponse::default();
        response.api_keys = vec![
            advertisement(API_VERSIONS_API_DESCRIPTOR.api_key.value(), 0, 0),
            advertisement(METADATA_API_DESCRIPTOR.api_key.value(), 0, 13),
            advertisement(CREATE_TOPICS_API_DESCRIPTOR.api_key.value(), 0, 7),
        ];
        self.write_response::<ApiVersionsRequest, _>(
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

    pub(super) fn respond<R, T>(&mut self, driver: &mut DriverOwner, response: &T, phase: &str)
    where
        R: RequestResponsePair<Response = T>,
        T: KafkaEncode,
    {
        let request = self.read_after_turns(driver, phase);
        assert_eq!(request.api_key, R::API_KEY.value());
        self.write_response::<R, _>(request.correlation_id, response, request.api_version);
        drive(driver, Duration::from_secs(1), phase);
    }

    pub(super) fn assert_no_frame(&self) {
        self.0
            .set_nonblocking(true)
            .unwrap_or_else(|error| panic!("make broker peer nonblocking: {error}"));
        let mut byte = [0; 1];
        match self.0.peek(&mut byte) {
            Ok(0) => {}
            Ok(observed) => panic!("visibility retry emitted {observed} byte before its deadline"),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("inspect broker request before retry: {error}"),
        }
        self.0
            .set_nonblocking(false)
            .unwrap_or_else(|error| panic!("restore blocking broker peer: {error}"));
    }

    fn read_after_turns(&mut self, driver: &mut DriverOwner, phase: &str) -> RequestFrame {
        self.0
            .set_nonblocking(true)
            .unwrap_or_else(|error| panic!("make broker peer nonblocking: {error}"));
        let mut byte = [0; 1];
        for _turn in 0..32 {
            drive(driver, Duration::from_millis(100), phase);
            match self.0.peek(&mut byte) {
                Ok(observed) if observed != 0 => {
                    self.0
                        .set_nonblocking(false)
                        .unwrap_or_else(|error| panic!("make broker peer blocking: {error}"));
                    return self.read_request();
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("inspect broker request: {error}"),
            }
        }
        panic!("{phase} did not produce a broker frame")
    }

    fn read_request(&mut self) -> RequestFrame {
        self.0
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap_or_else(|error| panic!("bound broker read: {error}"));
        let mut prefix = [0; size_of::<i32>()];
        self.0
            .read_exact(&mut prefix)
            .unwrap_or_else(|error| panic!("read frame length: {error}"));
        let length = usize::try_from(i32::from_be_bytes(prefix))
            .unwrap_or_else(|error| panic!("validate frame length: {error}"));
        let mut frame = vec![0; length];
        self.0
            .read_exact(&mut frame)
            .unwrap_or_else(|error| panic!("read request frame: {error}"));
        RequestFrame {
            api_key: read_i16(&frame, 0),
            api_version: ApiVersion::new(read_i16(&frame, 2)),
            correlation_id: read_i32(&frame, 4),
        }
    }

    fn write_response<R, T>(&mut self, correlation_id: i32, response: &T, version: ApiVersion)
    where
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
        self.0
            .write_all(&length.to_be_bytes())
            .and_then(|()| self.0.write_all(&body))
            .unwrap_or_else(|error| panic!("write response frame: {error}"));
    }
}

fn advertisement(api_key: i16, min_version: i16, max_version: i16) -> AdvertisedApi {
    let mut api = AdvertisedApi::default();
    api.api_key = api_key;
    api.min_version = min_version;
    api.max_version = max_version;
    api
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

fn drive(driver: &mut DriverOwner, wait: Duration, phase: &str) {
    driver
        .turn(wait)
        .unwrap_or_else(|error| panic!("{phase}: {error}"));
}

struct RequestFrame {
    api_key: i16,
    api_version: ApiVersion,
    correlation_id: i32,
}
