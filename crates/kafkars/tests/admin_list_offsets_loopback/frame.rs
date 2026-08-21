//! Generated request decoding and flexible response framing for Admin `ListOffsets`.

use std::io::{self, Read};

use bytes::{Bytes, BytesMut};
use kafka_wire::{
    KafkaRequest, RequestHeader, RequestResponsePair, ResponseHeader, request_header_version,
    response_header_version_for,
};
use kafka_wire_core::{ApiVersion, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

pub(super) struct RequestFrame {
    bytes: Bytes,
    pub(super) api_key: i16,
    pub(super) api_version: ApiVersion,
    pub(super) correlation_id: i32,
}

impl RequestFrame {
    pub(super) fn read(peer: &mut impl Read) -> io::Result<Self> {
        let mut prefix = [0; size_of::<i32>()];
        peer.read_exact(&mut prefix)?;
        let length = usize::try_from(i32::from_be_bytes(prefix))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let mut body = vec![0; length];
        peer.read_exact(&mut body)?;
        let bytes = Bytes::from(body);
        Ok(Self {
            api_key: read_i16(&bytes, 0)?,
            api_version: ApiVersion::new(read_i16(&bytes, 2)?),
            correlation_id: read_i32(&bytes, 4)?,
            bytes,
        })
    }

    pub(super) fn decode<R>(&self) -> R
    where
        R: KafkaRequest + KafkaDecode,
    {
        assert_eq!(self.api_key, R::API_KEY.value());
        let header_version = request_header_version(R::is_flexible(self.api_version));
        let mut decoder = Decoder::new(self.bytes.clone(), DecodeLimits::default())
            .unwrap_or_else(|error| panic!("construct ListOffsets request decoder: {error}"));
        let header = RequestHeader::decode(&mut decoder, ApiVersion::new(header_version))
            .unwrap_or_else(|error| panic!("decode ListOffsets request header: {error}"));
        assert_eq!(header.correlation_id, self.correlation_id);
        R::decode_from_bytes(
            self.bytes.slice(decoder.offset()..),
            self.api_version,
            DecodeLimits::default(),
        )
        .unwrap_or_else(|error| panic!("decode {} request: {error}", R::NAME))
    }
}

pub(super) fn encoded_response<R, T>(
    correlation_id: i32,
    response: &T,
    version: ApiVersion,
) -> Vec<u8>
where
    R: RequestResponsePair<Response = T>,
    T: KafkaEncode,
{
    let header_version = response_header_version_for::<R>(version)
        .unwrap_or_else(|error| panic!("ListOffsets response header policy: {error}"));
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;
    header
        .encode_into(&mut body, ApiVersion::new(header_version))
        .unwrap_or_else(|error| panic!("encode ListOffsets response header: {error}"));
    response
        .encode_into(&mut body, version)
        .unwrap_or_else(|error| panic!("encode ListOffsets response body: {error}"));
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("ListOffsets response length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

fn read_i16(bytes: &[u8], offset: usize) -> io::Result<i16> {
    bytes
        .get(offset..offset + 2)
        .and_then(|value| value.try_into().ok())
        .map(i16::from_be_bytes)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request i16"))
}

fn read_i32(bytes: &[u8], offset: usize) -> io::Result<i32> {
    bytes
        .get(offset..offset + 4)
        .and_then(|value| value.try_into().ok())
        .map(i32::from_be_bytes)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request i32"))
}
