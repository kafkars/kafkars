//! Strict one-topic, one-partition correlation around bounded Fetch decoding.

use kafka_wire::FetchResponse as WireFetchResponse;
use kafka_wire::fetch_response::PartitionData;

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

pub(super) fn validate_selected_version(selected_version: i16) -> Result<(), FetchResponseFailure> {
    if !(FETCH_NAME_ROUTE_MIN_VERSION..=FETCH_NAME_ROUTE_MAX_VERSION).contains(&selected_version) {
        return Err(FetchResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    Ok(())
}

pub(super) fn correlate_partition<'a>(
    topic: &str,
    partition: u32,
    response: &'a WireFetchResponse,
) -> Result<&'a PartitionData, FetchResponseFailure> {
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
    Ok(partition_response)
}

pub(super) fn normalize_correlated_response(
    response: WireFetchResponse,
    limits: FetchDecodeLimits,
) -> Result<FetchResponse, FetchResponseFailure> {
    normalize_fetch_response(response, limits).map_err(FetchResponseFailure::Decode)
}
