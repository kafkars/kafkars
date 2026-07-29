//! Linear generated request ownership for API-key 41 v1-v3.

use core::fmt;

use kafka_wire::{
    ApiDescriptor, DESCRIBE_DELEGATION_TOKEN_API_DESCRIPTOR, DescribeDelegationTokenRequest,
    DescribeDelegationTokenResponse, KafkaMessage, KafkaRequest, RequestResponsePair,
    RetainedFootprint, RetainedSize,
};
use kafka_wire_core::{
    ApiKey, ApiVersion, BytesMut, DecodeError, Decoder, EncodeError, EncodeTarget, Encoder,
    KafkaDecode, KafkaEncode, VersionRange,
};

/// One validated request that preserves nullable all-token selection.
#[must_use = "a prepared delegation-token query must be submitted or deliberately released"]
pub(crate) struct PreparedDescribeDelegationTokensRequest {
    request: DescribeDelegationTokenRequest,
}

impl PreparedDescribeDelegationTokensRequest {
    pub(super) const fn new(request: DescribeDelegationTokenRequest) -> Self {
        Self { request }
    }

    pub(crate) fn retained_heap_bytes(&self) -> usize {
        self.retained_size().heap_bytes()
    }
}

impl fmt::Debug for PreparedDescribeDelegationTokensRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedDescribeDelegationTokensRequest")
            .field(
                "selection",
                &self.request.owners.as_ref().map_or("all", |_| "selected"),
            )
            .field(
                "owner_count",
                &self.request.owners.as_ref().map_or(0, Vec::len),
            )
            .finish()
    }
}

impl KafkaDecode for PreparedDescribeDelegationTokensRequest {
    fn decode(decoder: &mut Decoder, version: ApiVersion) -> Result<Self, DecodeError> {
        DescribeDelegationTokenRequest::decode(decoder, version).map(Self::new)
    }
}

impl KafkaEncode for PreparedDescribeDelegationTokensRequest {
    fn encode<T: EncodeTarget>(
        &self,
        encoder: &mut Encoder<T>,
        version: ApiVersion,
    ) -> Result<(), EncodeError> {
        self.request.encode(encoder, version)
    }

    fn encoded_len(&self, version: ApiVersion) -> Result<usize, EncodeError> {
        self.request.encoded_len(version)
    }

    fn encode_into(
        &self,
        buffer: &mut BytesMut,
        version: ApiVersion,
    ) -> Result<usize, EncodeError> {
        self.request.encode_into(buffer, version)
    }
}

impl RetainedSize for PreparedDescribeDelegationTokensRequest {
    fn retained_size(&self) -> RetainedFootprint {
        self.request.retained_size()
    }
}

impl KafkaMessage for PreparedDescribeDelegationTokensRequest {
    const NAME: &'static str = <DescribeDelegationTokenRequest as KafkaMessage>::NAME;
    const SUPPORTED_VERSIONS: VersionRange =
        <DescribeDelegationTokenRequest as KafkaMessage>::SUPPORTED_VERSIONS;
    const FLEXIBLE_VERSIONS: Option<VersionRange> =
        <DescribeDelegationTokenRequest as KafkaMessage>::FLEXIBLE_VERSIONS;
}

impl KafkaRequest for PreparedDescribeDelegationTokensRequest {
    const API_KEY: ApiKey = <DescribeDelegationTokenRequest as KafkaRequest>::API_KEY;
    const API_DESCRIPTOR: &'static ApiDescriptor = &DESCRIBE_DELEGATION_TOKEN_API_DESCRIPTOR;
}

impl RequestResponsePair for PreparedDescribeDelegationTokensRequest {
    type Response = DescribeDelegationTokenResponse;
}
