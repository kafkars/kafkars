//! Strict one-topic, one-partition correlation around bounded Fetch decoding.

use kafka_wire::FetchResponse as WireFetchResponse;

use super::{
    decode::normalize_fetch_response, failure::FetchDecodeFailure, limits::FetchDecodeLimits,
    model::FetchResponse, request::FETCH_NAME_ROUTE_MAX_VERSION,
    request::FETCH_NAME_ROUTE_MIN_VERSION,
};

/// Why a generated response cannot settle one exact partition fetch.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum FetchResponseFailure {
    UnsupportedApiVersion { actual: i16 },
    RequestedPartitionOutOfRange { actual: u32 },
    TopicCount { actual: usize },
    TopicNameMismatch,
    PartitionCount { actual: usize },
    PartitionIndexMismatch { actual: i32 },
    Decode(FetchDecodeFailure),
}

/// Correlates one generated response before retaining any fact for a fetch fence.
pub(crate) fn normalize_one_partition_fetch_response(
    topic: &str,
    partition: u32,
    selected_version: i16,
    response: WireFetchResponse,
    limits: FetchDecodeLimits,
) -> Result<FetchResponse, FetchResponseFailure> {
    if !(FETCH_NAME_ROUTE_MIN_VERSION..=FETCH_NAME_ROUTE_MAX_VERSION).contains(&selected_version) {
        return Err(FetchResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let expected_partition = i32::try_from(partition)
        .map_err(|_| FetchResponseFailure::RequestedPartitionOutOfRange { actual: partition })?;
    let [topic_response] = response.responses.as_slice() else {
        return Err(FetchResponseFailure::TopicCount {
            actual: response.responses.len(),
        });
    };
    if topic_response.topic.as_str() != topic {
        return Err(FetchResponseFailure::TopicNameMismatch);
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

    normalize_fetch_response(response, limits).map_err(FetchResponseFailure::Decode)
}
