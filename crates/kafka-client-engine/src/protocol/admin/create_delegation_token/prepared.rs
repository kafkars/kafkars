//! Selected-version delegation to generated API-key 38 request encoding.

use core::fmt;

use kafka_wire::{
    ApiDescriptor, CREATE_DELEGATION_TOKEN_API_DESCRIPTOR, CreateDelegationTokenRequest,
    CreateDelegationTokenResponse, KafkaMessage, KafkaRequest, RequestResponsePair,
    RetainedFootprint, RetainedSize,
};
use kafka_wire_core::{
    ApiKey, ApiVersion, BytesMut, DecodeError, Decoder, EncodeError, EncodeTarget, Encoder,
    KafkaDecode, KafkaEncode, VersionRange,
};

pub(super) const DEFAULT_OWNER_MIN_VERSION: i16 = 1;
pub(super) const EXPLICIT_OWNER_MIN_VERSION: i16 = 3;

/// Generated request forms chosen only after the driver selects v1-v3.
#[must_use = "a prepared delegation-token mutation must be submitted or deliberately released"]
pub(crate) struct PreparedCreateDelegationTokenRequest {
    legacy: Option<CreateDelegationTokenRequest>,
    modern: CreateDelegationTokenRequest,
    minimum_version: i16,
}

impl PreparedCreateDelegationTokenRequest {
    pub(super) const fn new(
        legacy: Option<CreateDelegationTokenRequest>,
        modern: CreateDelegationTokenRequest,
        minimum_version: i16,
    ) -> Self {
        Self {
            legacy,
            modern,
            minimum_version,
        }
    }

    pub(crate) const fn minimum_version(&self) -> i16 {
        self.minimum_version
    }

    pub(crate) fn retained_heap_bytes(&self) -> usize {
        self.retained_size().heap_bytes()
    }

    fn request_for_version(
        &self,
        version: ApiVersion,
    ) -> Result<&CreateDelegationTokenRequest, EncodeError> {
        match version.value() {
            1 | 2 => self.legacy.as_ref().ok_or(EncodeError::UnsupportedVersion {
                message: Self::NAME,
                version,
                supported: Self::SUPPORTED_VERSIONS,
            }),
            3 => Ok(&self.modern),
            _ => Err(EncodeError::UnsupportedVersion {
                message: Self::NAME,
                version,
                supported: Self::SUPPORTED_VERSIONS,
            }),
        }
    }
}

#[allow(
    clippy::missing_fields_in_debug,
    reason = "debug output deliberately summarizes generated requests without exposing identities"
)]
impl fmt::Debug for PreparedCreateDelegationTokenRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedCreateDelegationTokenRequest")
            .field(
                "explicit_owner",
                &(self.minimum_version == EXPLICIT_OWNER_MIN_VERSION),
            )
            .field("renewers", &self.modern.renewers.len())
            .field("max_lifetime_ms", &self.modern.max_lifetime_ms)
            .finish()
    }
}

impl KafkaDecode for PreparedCreateDelegationTokenRequest {
    fn decode(decoder: &mut Decoder, version: ApiVersion) -> Result<Self, DecodeError> {
        let request = CreateDelegationTokenRequest::decode(decoder, version)?;
        match version.value() {
            1 | 2 => {
                let mut modern = request.clone();
                modern.owner_principal_type = None;
                modern.owner_principal_name = None;
                Ok(Self::new(Some(request), modern, DEFAULT_OWNER_MIN_VERSION))
            }
            3 => {
                let minimum_version = if request.owner_principal_type.is_some()
                    || request.owner_principal_name.is_some()
                {
                    EXPLICIT_OWNER_MIN_VERSION
                } else {
                    DEFAULT_OWNER_MIN_VERSION
                };
                Ok(Self::new(None, request, minimum_version))
            }
            _ => unreachable!("generated decode rejects versions outside v1-v3"),
        }
    }
}

impl KafkaEncode for PreparedCreateDelegationTokenRequest {
    fn encode<T: EncodeTarget>(
        &self,
        encoder: &mut Encoder<T>,
        version: ApiVersion,
    ) -> Result<(), EncodeError> {
        self.request_for_version(version)?.encode(encoder, version)
    }

    fn encoded_len(&self, version: ApiVersion) -> Result<usize, EncodeError> {
        self.request_for_version(version)?.encoded_len(version)
    }

    fn encode_into(
        &self,
        buffer: &mut BytesMut,
        version: ApiVersion,
    ) -> Result<usize, EncodeError> {
        self.request_for_version(version)?
            .encode_into(buffer, version)
    }
}

impl RetainedSize for PreparedCreateDelegationTokenRequest {
    fn retained_size(&self) -> RetainedFootprint {
        self.legacy
            .retained_size()
            .saturating_add(self.modern.retained_size())
    }
}

impl KafkaMessage for PreparedCreateDelegationTokenRequest {
    const NAME: &'static str = <CreateDelegationTokenRequest as KafkaMessage>::NAME;
    const SUPPORTED_VERSIONS: VersionRange =
        <CreateDelegationTokenRequest as KafkaMessage>::SUPPORTED_VERSIONS;
    const FLEXIBLE_VERSIONS: Option<VersionRange> =
        <CreateDelegationTokenRequest as KafkaMessage>::FLEXIBLE_VERSIONS;
}

impl KafkaRequest for PreparedCreateDelegationTokenRequest {
    const API_KEY: ApiKey = <CreateDelegationTokenRequest as KafkaRequest>::API_KEY;
    const API_DESCRIPTOR: &'static ApiDescriptor = &CREATE_DELEGATION_TOKEN_API_DESCRIPTOR;
}

impl RequestResponsePair for PreparedCreateDelegationTokenRequest {
    type Response = CreateDelegationTokenResponse;
}
