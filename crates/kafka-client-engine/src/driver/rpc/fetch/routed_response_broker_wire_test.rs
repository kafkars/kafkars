//! Pure Kafka frame and response facts for the routed loopback broker.

use bytes::{Bytes, BytesMut};
use kafka_driver::ApiVersion;
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsResponse, FETCH_API_DESCRIPTOR,
    METADATA_API_DESCRIPTOR, MetadataResponse, PRODUCE_API_DESCRIPTOR, RequestResponsePair,
    ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    response_header_version_for,
};
use kafka_wire_core::{KafkaEncode, StrBytes, Uuid};

pub(super) fn api_versions_response() -> ApiVersionsResponse {
    let mut response = ApiVersionsResponse::default();
    response.api_keys = vec![
        advertisement(API_VERSIONS_API_DESCRIPTOR.api_key.value(), 0, 0),
        advertisement(METADATA_API_DESCRIPTOR.api_key.value(), 0, 13),
        advertisement(FETCH_API_DESCRIPTOR.api_key.value(), 4, 16),
        advertisement(PRODUCE_API_DESCRIPTOR.api_key.value(), 3, 12),
    ];
    response
}

pub(super) fn encode_response<R, T>(
    correlation_id: i32,
    response: &T,
    version: ApiVersion,
) -> BytesMut
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
    body
}

pub(super) fn metadata_response(
    port: u16,
    include_partition: bool,
    topic_name: &str,
    partition_count: i32,
    all_partitions_available: bool,
) -> MetadataResponse {
    let mut response = MetadataResponse::default();
    response.brokers.push(broker(port));
    response.controller_id = 1;
    if include_partition {
        let mut topic = MetadataResponseTopic::default();
        topic.name = Some(StrBytes::from(topic_name.to_owned()));
        topic.topic_id = Uuid::from_bytes([7; 16]);
        for partition_index in 0..partition_count {
            let mut partition = MetadataResponsePartition::default();
            partition.partition_index = partition_index;
            let available = all_partitions_available || partition_index == partition_count - 1;
            partition.leader_id = if available { 1 } else { -1 };
            partition.leader_epoch = if available { 9 } else { -1 };
            topic.partitions.push(partition);
        }
        response.topics.push(topic);
    }
    response
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

impl RequestFrame {
    pub(super) fn from_bytes(frame: Vec<u8>) -> Self {
        Self {
            api_key: read_i16(&frame, 0),
            api_version: ApiVersion::new(read_i16(&frame, 2)),
            correlation_id: read_i32(&frame, 4),
            bytes: Bytes::from(frame),
        }
    }
}
