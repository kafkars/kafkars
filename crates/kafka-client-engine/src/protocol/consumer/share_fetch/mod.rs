//! Strict generated-wire confinement for KIP-932 `ShareFetch` v1.
#![allow(
    dead_code,
    reason = "the protocol checkpoint precedes tracked driver and hosted session ownership"
)]

mod failure;
mod model;
mod request;
mod request_plan;
mod response;
mod response_partition;

pub(crate) use failure::{ShareFetchRequestFailure, ShareFetchResponseFailure};
pub(crate) use model::{
    SHARE_FETCH_MAX_ENDPOINTS, SHARE_FETCH_MAX_PARTITIONS, SHARE_FETCH_MAX_RANGES,
    SHARE_FETCH_MAX_TOPICS, SHARE_FETCH_MAX_VERSION, SHARE_FETCH_MIN_VERSION,
    ShareFetchAcquiredRange, ShareFetchBrokerRejection, ShareFetchCorrelation, ShareFetchEndpoint,
    ShareFetchOutcome, ShareFetchPartition, ShareFetchPartitionRejection, ShareFetchResponseLimits,
    ShareFetchSuccess, ShareFetchTopic,
};
#[expect(
    unused_imports,
    reason = "the tracked ShareFetch driver call lands in the next checkpoint"
)]
pub(crate) use request::{
    PreparedShareFetchRequest, ShareFetchRequestSettings, share_fetch_close_request,
    share_fetch_request,
};
pub(crate) use request_plan::{ShareFetchRequestPlan, ShareFetchRequestTopic};
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "the tracked ShareFetch driver settlement lands in the next checkpoint"
    )
)]
pub(crate) use response::normalize_share_fetch_response;

#[cfg(test)]
mod request_plan_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_partition_test;
#[cfg(test)]
mod response_test;
