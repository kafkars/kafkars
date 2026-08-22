//! Strict one-topic, one-partition correlation around bounded Fetch decoding.

use kafka_wire::FetchResponse as WireFetchResponse;
use kafka_wire::fetch_response::PartitionData;

use super::{
    decode::{normalize_fetch_response, normalize_leader},
    failure::FetchDecodeFailure,
    limits::FetchDecodeLimits,
    model::{FetchLeader, FetchResponse},
    request::FETCH_NAME_ROUTE_MAX_VERSION,
    request::FETCH_NAME_ROUTE_MIN_VERSION,
    request::FETCH_TOPIC_ID_ROUTE_VERSION,
};

/// Why a generated response cannot settle one exact partition fetch.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum FetchResponseFailure {
    UnsupportedApiVersion { actual: i16 },
    RequestedPartitionOutOfRange { actual: u32 },
    TopicCount { actual: usize },
    TopicNameMismatch,
    TopicIdMismatch,
    PartitionCount { actual: usize },
    PartitionIndexMismatch { actual: i32 },
    Decode(FetchDecodeFailure),
}

pub(super) fn partition_leader_hint(
    partition: &PartitionData,
) -> Result<Option<FetchLeader>, FetchResponseFailure> {
    normalize_leader(
        partition.current_leader.leader_id,
        partition.current_leader.leader_epoch,
    )
    .map_err(FetchResponseFailure::Decode)
}

pub(super) fn validate_selected_version(selected_version: i16) -> Result<(), FetchResponseFailure> {
    if !(FETCH_NAME_ROUTE_MIN_VERSION..=FETCH_NAME_ROUTE_MAX_VERSION).contains(&selected_version)
        && selected_version != FETCH_TOPIC_ID_ROUTE_VERSION
    {
        return Err(FetchResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    Ok(())
}

pub(super) fn correlate_partition<'a>(
    topic: &str,
    topic_id: Option<[u8; 16]>,
    partition: u32,
    selected_version: i16,
    response: &'a WireFetchResponse,
) -> Result<&'a PartitionData, FetchResponseFailure> {
    let expected_partition = i32::try_from(partition)
        .map_err(|_| FetchResponseFailure::RequestedPartitionOutOfRange { actual: partition })?;
    let [topic_response] = response.responses.as_slice() else {
        return Err(FetchResponseFailure::TopicCount {
            actual: response.responses.len(),
        });
    };
    if selected_version <= FETCH_NAME_ROUTE_MAX_VERSION {
        if topic_response.topic.as_str() != topic {
            return Err(FetchResponseFailure::TopicNameMismatch);
        }
    } else if topic_id != Some(topic_response.topic_id.to_bytes()) {
        return Err(FetchResponseFailure::TopicIdMismatch);
    }
    let [partition_response] = topic_response.partitions.as_slice() else {
        return Err(FetchResponseFailure::PartitionCount {
            actual: topic_response.partitions.len(),
        });
    };
    if partition_response.partition_index != expected_partition {
        return Err(FetchResponseFailure::PartitionIndexMismatch {
            actual: partition_response.partition_index,
        });
    }
    Ok(partition_response)
}

pub(super) fn normalize_correlated_response(
    response: WireFetchResponse,
    limits: FetchDecodeLimits,
) -> Result<FetchResponse, FetchResponseFailure> {
    normalize_fetch_response(response, limits).map_err(FetchResponseFailure::Decode)
}
