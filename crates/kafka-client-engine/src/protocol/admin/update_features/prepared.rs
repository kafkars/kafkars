//! Allocation-free selected-version encoding for one prepared feature mutation.

use kafka_wire::{
    ApiDescriptor, KafkaMessage, KafkaRequest, RequestResponsePair, RetainedFootprint,
    RetainedSize, UPDATE_FEATURES_API_DESCRIPTOR, UpdateFeaturesRequest, UpdateFeaturesResponse,
};
use kafka_wire_core::{
    ApiKey, ApiVersion, BytesMut, DecodeError, Decoder, EncodeError, EncodeTarget, Encoder,
    KafkaDecode, KafkaEncode, VersionRange,
};

/// Version-adaptive request ownership accepted directly by `kafka-driver`.
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use = "a prepared feature mutation must be submitted or deliberately released"]
pub(crate) struct PreparedUpdateFeaturesRequest {
    legacy: Option<UpdateFeaturesRequest>,
    modern: UpdateFeaturesRequest,
}

impl PreparedUpdateFeaturesRequest {
    pub(super) const fn new(
        legacy: Option<UpdateFeaturesRequest>,
        modern: UpdateFeaturesRequest,
    ) -> Self {
        Self { legacy, modern }
    }

    /// Reports all generated request ownership retained for selected-version encoding.
    pub(crate) fn retained_heap_bytes(&self) -> usize {
        self.retained_size().heap_bytes()
    }

    fn request_for_version(
        &self,
        version: ApiVersion,
    ) -> Result<&UpdateFeaturesRequest, EncodeError> {
        if version.value() == 0 {
            self.legacy.as_ref().ok_or(EncodeError::UnsupportedVersion {
                message: Self::NAME,
                version,
                supported: Self::SUPPORTED_VERSIONS,
            })
        } else {
            Ok(&self.modern)
        }
    }
}

impl KafkaDecode for PreparedUpdateFeaturesRequest {
    fn decode(decoder: &mut Decoder, version: ApiVersion) -> Result<Self, DecodeError> {
        let request = UpdateFeaturesRequest::decode(decoder, version)?;
        if version.value() == 0 {
            let legacy = request;
            let mut modern = legacy.clone();
            for update in &mut modern.feature_updates {
                update.upgrade_type = if update.allow_downgrade { 2 } else { 1 };
                update.allow_downgrade = false;
            }
            Ok(Self::new(Some(legacy), modern))
        } else {
            Ok(Self::new(None, request))
        }
    }
}

impl KafkaEncode for PreparedUpdateFeaturesRequest {
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

impl RetainedSize for PreparedUpdateFeaturesRequest {
    fn retained_size(&self) -> RetainedFootprint {
        self.legacy
            .retained_size()
            .saturating_add(self.modern.retained_size())
    }
}

impl KafkaMessage for PreparedUpdateFeaturesRequest {
    const NAME: &'static str = <UpdateFeaturesRequest as KafkaMessage>::NAME;
    const SUPPORTED_VERSIONS: VersionRange =
        <UpdateFeaturesRequest as KafkaMessage>::SUPPORTED_VERSIONS;
    const FLEXIBLE_VERSIONS: Option<VersionRange> =
        <UpdateFeaturesRequest as KafkaMessage>::FLEXIBLE_VERSIONS;
}

impl KafkaRequest for PreparedUpdateFeaturesRequest {
    const API_KEY: ApiKey = <UpdateFeaturesRequest as KafkaRequest>::API_KEY;
    const API_DESCRIPTOR: &'static ApiDescriptor = &UPDATE_FEATURES_API_DESCRIPTOR;
}

impl RequestResponsePair for PreparedUpdateFeaturesRequest {
    type Response = UpdateFeaturesResponse;
}

#[cfg(test)]
impl PreparedUpdateFeaturesRequest {
    pub(super) fn request_for_test(&self, version: i16) -> Option<&UpdateFeaturesRequest> {
        match version {
            0 => self.legacy.as_ref(),
            1 | 2 => Some(&self.modern),
            _ => None,
        }
    }
}
