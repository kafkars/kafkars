//! Generated `OffsetFetch` delegation across the legacy v7 and modern v8 split.

use kafka_wire::{
    KafkaMessage, KafkaRequest, OffsetFetchRequest, OffsetFetchResponse, RequestResponsePair,
    RetainedFootprint, RetainedSize,
    offset_fetch_request::{
        OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
    },
};
use kafka_wire_core::{
    ApiKey, ApiVersion, DecodeError, Decoder, EncodeError, EncodeTarget, Encoder, KafkaDecode,
    KafkaEncode, StrBytes, VersionRange,
};

use super::model::GroupOffsetFetchTopic;

/// One explicit assigned-partition query backed by generated schema branches.
#[must_use = "a generated group offset fetch request must be submitted or released"]
pub(crate) struct GroupOffsetFetchRequest {
    representation: GroupOffsetFetchRequestRepresentation,
}

enum GroupOffsetFetchRequestRepresentation {
    Prepared {
        legacy: OffsetFetchRequest,
        modern: OffsetFetchRequest,
    },
    Decoded(OffsetFetchRequest),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupOffsetFetchRequestBuildFailure {
    Allocation,
}

pub(super) fn try_group_offset_fetch_request(
    group_id: &str,
    topics: &[GroupOffsetFetchTopic],
) -> Result<GroupOffsetFetchRequest, GroupOffsetFetchRequestBuildFailure> {
    let mut legacy_topics = reserved(topics.len())?;
    let mut modern_topics = reserved(topics.len())?;
    for topic in topics {
        let mut legacy = OffsetFetchRequestTopic::default();
        legacy.name = try_string(topic.name())?;
        legacy.partition_indexes = copied_partitions(topic.partition_indexes())?;
        legacy_topics.push(legacy);

        let mut modern = OffsetFetchRequestTopics::default();
        modern.name = try_string(topic.name())?;
        modern.partition_indexes = copied_partitions(topic.partition_indexes())?;
        modern_topics.push(modern);
    }

    let mut legacy = OffsetFetchRequest::default();
    legacy.group_id = try_string(group_id)?;
    legacy.topics = Some(legacy_topics);
    legacy.require_stable = false;

    let mut group = OffsetFetchRequestGroup::default();
    group.group_id = try_string(group_id)?;
    group.topics = Some(modern_topics);
    let mut groups = reserved(1)?;
    groups.push(group);
    let mut modern = OffsetFetchRequest::default();
    modern.groups = groups;
    modern.require_stable = false;

    Ok(GroupOffsetFetchRequest {
        representation: GroupOffsetFetchRequestRepresentation::Prepared { legacy, modern },
    })
}

fn reserved<T>(capacity: usize) -> Result<Vec<T>, GroupOffsetFetchRequestBuildFailure> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| GroupOffsetFetchRequestBuildFailure::Allocation)?;
    Ok(values)
}

fn copied_partitions(partitions: &[i32]) -> Result<Vec<i32>, GroupOffsetFetchRequestBuildFailure> {
    let mut owned = reserved(partitions.len())?;
    owned.extend_from_slice(partitions);
    Ok(owned)
}

fn try_string(value: &str) -> Result<StrBytes, GroupOffsetFetchRequestBuildFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| GroupOffsetFetchRequestBuildFailure::Allocation)?;
    owned.push_str(value);
    Ok(owned.into())
}

impl GroupOffsetFetchRequest {
    pub(super) fn retained_bytes(&self) -> usize {
        self.retained_size().heap_bytes()
    }

    fn generated(&self, version: ApiVersion) -> &OffsetFetchRequest {
        match &self.representation {
            GroupOffsetFetchRequestRepresentation::Prepared { legacy, .. }
                if version.value() <= 7 =>
            {
                legacy
            }
            GroupOffsetFetchRequestRepresentation::Prepared { modern, .. } => modern,
            GroupOffsetFetchRequestRepresentation::Decoded(request) => request,
        }
    }
}

impl KafkaEncode for GroupOffsetFetchRequest {
    fn encode<T: EncodeTarget>(
        &self,
        encoder: &mut Encoder<T>,
        version: ApiVersion,
    ) -> Result<(), EncodeError> {
        if !Self::SUPPORTED_VERSIONS.contains(version) {
            return Err(EncodeError::UnsupportedVersion {
                message: Self::NAME,
                version,
                supported: Self::SUPPORTED_VERSIONS,
            });
        }
        self.generated(version).encode(encoder, version)
    }
}

impl KafkaDecode for GroupOffsetFetchRequest {
    fn decode(decoder: &mut Decoder, version: ApiVersion) -> Result<Self, DecodeError> {
        if !Self::SUPPORTED_VERSIONS.contains(version) {
            return Err(DecodeError::UnsupportedVersion {
                message: Self::NAME,
                version,
                supported: Self::SUPPORTED_VERSIONS,
            });
        }
        OffsetFetchRequest::decode(decoder, version).map(|request| Self {
            representation: GroupOffsetFetchRequestRepresentation::Decoded(request),
        })
    }
}

impl RetainedSize for GroupOffsetFetchRequest {
    fn retained_size(&self) -> RetainedFootprint {
        match &self.representation {
            GroupOffsetFetchRequestRepresentation::Prepared { legacy, modern } => legacy
                .retained_size()
                .saturating_add(modern.retained_size()),
            GroupOffsetFetchRequestRepresentation::Decoded(request) => request.retained_size(),
        }
    }
}

impl KafkaMessage for GroupOffsetFetchRequest {
    const NAME: &'static str = "OffsetFetchRequest";
    const SUPPORTED_VERSIONS: VersionRange = VersionRange::new(2, 9);
    const FLEXIBLE_VERSIONS: Option<VersionRange> = Some(VersionRange::new(6, 9));
}

impl KafkaRequest for GroupOffsetFetchRequest {
    const API_KEY: ApiKey = ApiKey::new(9);
    const API_DESCRIPTOR: &'static kafka_wire::ApiDescriptor =
        <OffsetFetchRequest as KafkaRequest>::API_DESCRIPTOR;
}

impl RequestResponsePair for GroupOffsetFetchRequest {
    type Response = OffsetFetchResponse;
}
