//! Secret-safe delegation to generated API-key 40 request encoding.

use core::fmt;

use kafka_wire::{
    ApiDescriptor, EXPIRE_DELEGATION_TOKEN_API_DESCRIPTOR, ExpireDelegationTokenRequest,
    ExpireDelegationTokenResponse, KafkaMessage, KafkaRequest, RequestResponsePair,
    RetainedFootprint, RetainedSize,
};
use kafka_wire_core::{
    ApiKey, ApiVersion, Bytes, BytesMut, DecodeError, Decoder, EncodeError, EncodeTarget, Encoder,
    KafkaDecode, KafkaEncode, VersionRange,
};
use zeroize::Zeroize;

use super::secret::ExpireDelegationTokenHmac;

/// Owned request with one unique zeroizing secret and generated-only encoding.
#[must_use = "a prepared delegation-token expiration must be submitted or deliberately released"]
pub(crate) struct PreparedExpireDelegationTokenRequest {
    hmac: ExpireDelegationTokenHmac,
    expiry_time_period_ms: i64,
}

impl PreparedExpireDelegationTokenRequest {
    pub(super) const fn new(hmac: ExpireDelegationTokenHmac, expiry_time_period_ms: i64) -> Self {
        Self {
            hmac,
            expiry_time_period_ms,
        }
    }

    pub(crate) fn retained_heap_bytes(&self) -> usize {
        self.retained_size().heap_bytes()
    }

    fn generated(&self) -> ZeroizingGeneratedRequest {
        let mut request = ExpireDelegationTokenRequest::default();
        request.hmac = Bytes::from(self.hmac.as_bytes().to_vec());
        request.expiry_time_period_ms = self.expiry_time_period_ms;
        ZeroizingGeneratedRequest(request)
    }

    #[cfg(test)]
    pub(super) fn hmac_for_test(&self) -> &[u8] {
        self.hmac.as_bytes()
    }

    #[cfg(test)]
    pub(super) fn zeroize_hmac_for_test(&mut self) {
        self.hmac.zeroize_for_test();
    }
}

impl fmt::Debug for PreparedExpireDelegationTokenRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedExpireDelegationTokenRequest")
            .field("hmac", &self.hmac)
            .field("expiry_time_period_ms", &self.expiry_time_period_ms)
            .finish()
    }
}

impl KafkaDecode for PreparedExpireDelegationTokenRequest {
    fn decode(decoder: &mut Decoder, version: ApiVersion) -> Result<Self, DecodeError> {
        let request = ExpireDelegationTokenRequest::decode(decoder, version)?;
        Ok(Self::new(
            ExpireDelegationTokenHmac::from_decoded(request.hmac.as_ref()),
            request.expiry_time_period_ms,
        ))
    }
}

impl KafkaEncode for PreparedExpireDelegationTokenRequest {
    fn encode<T: EncodeTarget>(
        &self,
        encoder: &mut Encoder<T>,
        version: ApiVersion,
    ) -> Result<(), EncodeError> {
        self.generated().0.encode(encoder, version)
    }

    fn encoded_len(&self, version: ApiVersion) -> Result<usize, EncodeError> {
        self.generated().0.encoded_len(version)
    }

    fn encode_into(
        &self,
        buffer: &mut BytesMut,
        version: ApiVersion,
    ) -> Result<usize, EncodeError> {
        self.generated().0.encode_into(buffer, version)
    }
}

impl RetainedSize for PreparedExpireDelegationTokenRequest {
    fn retained_size(&self) -> RetainedFootprint {
        RetainedFootprint::allocation(self.hmac.retained_capacity())
    }
}

impl KafkaMessage for PreparedExpireDelegationTokenRequest {
    const NAME: &'static str = <ExpireDelegationTokenRequest as KafkaMessage>::NAME;
    const SUPPORTED_VERSIONS: VersionRange =
        <ExpireDelegationTokenRequest as KafkaMessage>::SUPPORTED_VERSIONS;
    const FLEXIBLE_VERSIONS: Option<VersionRange> =
        <ExpireDelegationTokenRequest as KafkaMessage>::FLEXIBLE_VERSIONS;
}

impl KafkaRequest for PreparedExpireDelegationTokenRequest {
    const API_KEY: ApiKey = <ExpireDelegationTokenRequest as KafkaRequest>::API_KEY;
    const API_DESCRIPTOR: &'static ApiDescriptor = &EXPIRE_DELEGATION_TOKEN_API_DESCRIPTOR;
}

impl RequestResponsePair for PreparedExpireDelegationTokenRequest {
    type Response = ExpireDelegationTokenResponse;
}

struct ZeroizingGeneratedRequest(ExpireDelegationTokenRequest);

impl Drop for ZeroizingGeneratedRequest {
    fn drop(&mut self) {
        let hmac = core::mem::take(&mut self.0.hmac);
        if let Ok(mut bytes) = hmac.try_into_mut() {
            bytes.as_mut().zeroize();
        }
    }
}
