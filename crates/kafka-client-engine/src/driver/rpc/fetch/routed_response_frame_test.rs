//! Generated request decoding for the routed Fetch loopback broker.

use kafka_wire::{KafkaRequest, RequestHeader, request_header_version};
use kafka_wire_core::{ApiVersion, DecodeLimits, Decoder, KafkaDecode};

use super::routed_response_broker_wire_test::RequestFrame;

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
