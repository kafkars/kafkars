//! Bounded Fetch-response normalization into engine-owned retained records.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "direct-consumer host interpretation follows this bounded protocol seam"
    )
)]

mod batch;
mod batch_identity;
mod batch_model;
mod decode;
mod failure;
mod limits;
mod model;
mod request;
mod response;

#[allow(
    unused_imports,
    reason = "the direct-consumer interpreter will consume this complete retained seam"
)]
pub(crate) use failure::FetchDecodeFailure;
#[allow(
    unused_imports,
    reason = "the direct-consumer interpreter will consume this complete retained seam"
)]
pub(crate) use limits::FetchDecodeLimits;
#[allow(
    unused_imports,
    reason = "the direct-consumer interpreter will consume this complete retained seam"
)]
pub(crate) use model::{
    FetchBatch, FetchEndpoint, FetchHeader, FetchPartition, FetchProducerIdentity, FetchRecord,
    FetchResponse, FetchTimestampType, FetchTopic,
};
#[allow(
    unused_imports,
    reason = "the direct-consumer interpreter will consume this request seam"
)]
pub(crate) use request::{
    FETCH_NAME_ROUTE_MAX_VERSION, FETCH_NAME_ROUTE_MIN_VERSION, FetchRequestFailure,
    FetchRequestSettings, fetch_request,
};
#[allow(
    unused_imports,
    reason = "the direct-consumer interpreter will consume this correlated response seam"
)]
pub(crate) use response::{FetchResponseFailure, normalize_one_partition_fetch_response};

#[cfg(test)]
mod batch_identity_test;
#[cfg(test)]
mod batch_model_test;
#[cfg(test)]
mod batch_test;
#[cfg(test)]
mod decode_next_test;
#[cfg(test)]
mod decode_test;
#[cfg(test)]
mod facts_test;
#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod limits_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
