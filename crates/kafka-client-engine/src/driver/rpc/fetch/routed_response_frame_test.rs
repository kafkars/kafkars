//! Generated request decoding and metadata facts for the routed Fetch loopback broker.

use kafka_wire::{
    KafkaRequest, RequestHeader, api_versions_response::ApiVersion as AdvertisedApi,
    metadata_response::MetadataResponseBroker, request_header_version,
};
use kafka_wire_core::{ApiVersion, DecodeLimits, Decoder, KafkaDecode, StrBytes};

use super::routed_response_broker_test::RequestFrame;

pub(super) fn advertisement(api_key: i16, min_version: i16, max_version: i16) -> AdvertisedApi {
    let mut api = AdvertisedApi::default();
    api.api_key = api_key;
    api.min_version = min_version;
    api.max_version = max_version;
    api
}

pub(super) fn broker(port: u16) -> MetadataResponseBroker {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 1;
    broker.host = StrBytes::from("127.0.0.1");
    broker.port = i32::from(port);
    broker
}

impl RequestFrame {
    pub(super) fn decode<R>(&self) -> R
    where
        R: KafkaRequest + KafkaDecode,
    {
        let header_version = request_header_version(R::is_flexible(self.api_version));
        let mut decoder = Decoder::new(self.bytes.clone(), DecodeLimits::default())
            .unwrap_or_else(|error| panic!("construct request decoder: {error}"));
        RequestHeader::decode(&mut decoder, ApiVersion::new(header_version))
            .unwrap_or_else(|error| panic!("decode request header: {error}"));
        R::decode_from_bytes(
            self.bytes.slice(decoder.offset()..),
            self.api_version,
            DecodeLimits::default(),
        )
        .unwrap_or_else(|error| panic!("decode {} request: {error}", R::NAME))
    }
}
