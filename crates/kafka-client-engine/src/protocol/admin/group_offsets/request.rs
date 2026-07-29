//! Version-selecting generated `OffsetFetch` request adaptation across v7/v8.

use std::collections::BTreeMap;

use kafka_client_core::ListConsumerGroupOffsetsSelection;
use kafka_wire::{
    KafkaMessage, KafkaRequest, OffsetFetchRequest, OffsetFetchResponse, RequestResponsePair,
    RetainedFootprint, RetainedSize,
    offset_fetch_request::{
        OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
    },
};
use kafka_wire_core::{
    ApiKey, ApiVersion, DecodeError, Decoder, EncodeError, EncodeTarget, Encoder, KafkaDecode,
    KafkaEncode, VersionRange,
};

/// One group-wide semantic query backed by both generated schema branches.
///
/// Kafka moves the legacy `group_id` and nullable `topics` fields into a
/// per-group array in v8. Keeping both generated DTOs lets the driver negotiate
/// v2-v9 while `kafka-wire` remains the sole encoder for every selected version.
#[must_use = "a generated group-offset query must be submitted or released"]
pub(crate) struct GroupOffsetsRequest {
    representation: GroupOffsetsRequestRepresentation,
}

enum GroupOffsetsRequestRepresentation {
    Prepared {
        legacy: OffsetFetchRequest,
        modern: OffsetFetchRequest,
    },
    Decoded(OffsetFetchRequest),
}

/// Builds one API-key 9 query without routing or retry policy.
pub(crate) fn group_offsets_request(
    group_id: &str,
    selection: &ListConsumerGroupOffsetsSelection,
    require_stable: bool,
) -> GroupOffsetsRequest {
    let legacy_topics = request_topics(selection);
    let mut legacy = OffsetFetchRequest::default();
    legacy.group_id = group_id.into();
    legacy.topics = legacy_topics.clone();
    legacy.require_stable = require_stable;

    let mut group = OffsetFetchRequestGroup::default();
    group.group_id = group_id.into();
    group.topics = legacy_topics.map(|topics| {
        topics
            .into_iter()
            .map(|topic| {
                let mut modern = OffsetFetchRequestTopics::default();
                modern.name = topic.name;
                modern.partition_indexes = topic.partition_indexes;
                modern
            })
            .collect()
    });
    let mut modern = OffsetFetchRequest::default();
    modern.groups = vec![group];
    modern.require_stable = require_stable;

    GroupOffsetsRequest {
        representation: GroupOffsetsRequestRepresentation::Prepared { legacy, modern },
    }
}

fn request_topics(
    selection: &ListConsumerGroupOffsetsSelection,
) -> Option<Vec<OffsetFetchRequestTopic>> {
    let ListConsumerGroupOffsetsSelection::Selected(targets) = selection else {
        return None;
    };
    let mut topics = Vec::<OffsetFetchRequestTopic>::new();
    let mut topic_positions = BTreeMap::<&str, usize>::new();
    for target in targets {
        if let Some(position) = topic_positions.get(target.topic()).copied() {
            let topic = &mut topics[position];
            topic.partition_indexes.push(target.partition());
            continue;
        }
        topic_positions.insert(target.topic(), topics.len());
        let mut topic = OffsetFetchRequestTopic::default();
        topic.name = target.topic().into();
        topic.partition_indexes = vec![target.partition()];
        topics.push(topic);
    }
    Some(topics)
}

impl GroupOffsetsRequest {
    fn generated(&self, version: ApiVersion) -> &OffsetFetchRequest {
        match &self.representation {
            GroupOffsetsRequestRepresentation::Prepared { legacy, .. } if version.value() <= 7 => {
                legacy
            }
            GroupOffsetsRequestRepresentation::Prepared { modern, .. } => modern,
            GroupOffsetsRequestRepresentation::Decoded(request) => request,
        }
    }
}

impl KafkaEncode for GroupOffsetsRequest {
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

impl KafkaDecode for GroupOffsetsRequest {
    fn decode(decoder: &mut Decoder, version: ApiVersion) -> Result<Self, DecodeError> {
        if !Self::SUPPORTED_VERSIONS.contains(version) {
            return Err(DecodeError::UnsupportedVersion {
                message: Self::NAME,
                version,
                supported: Self::SUPPORTED_VERSIONS,
            });
        }
        OffsetFetchRequest::decode(decoder, version).map(|request| Self {
            representation: GroupOffsetsRequestRepresentation::Decoded(request),
        })
    }
}

impl RetainedSize for GroupOffsetsRequest {
    fn retained_size(&self) -> RetainedFootprint {
        match &self.representation {
            GroupOffsetsRequestRepresentation::Prepared { legacy, modern } => legacy
                .retained_size()
                .saturating_add(modern.retained_size()),
            GroupOffsetsRequestRepresentation::Decoded(request) => request.retained_size(),
        }
    }
}

impl KafkaMessage for GroupOffsetsRequest {
    const NAME: &'static str = "OffsetFetchRequest";
    const SUPPORTED_VERSIONS: VersionRange = VersionRange::new(2, 9);
    const FLEXIBLE_VERSIONS: Option<VersionRange> = Some(VersionRange::new(6, 9));
}

impl KafkaRequest for GroupOffsetsRequest {
    const API_KEY: ApiKey = ApiKey::new(9);
    const API_DESCRIPTOR: &'static kafka_wire::ApiDescriptor =
        <OffsetFetchRequest as KafkaRequest>::API_DESCRIPTOR;
}

impl RequestResponsePair for GroupOffsetsRequest {
    type Response = OffsetFetchResponse;
}
