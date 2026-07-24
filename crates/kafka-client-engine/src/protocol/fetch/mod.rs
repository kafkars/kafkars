//! Bounded Fetch-response normalization into engine-owned retained records.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "direct-consumer host interpretation follows this bounded protocol seam"
    )
)]

mod batch;
mod decode;
mod failure;
mod limits;
mod model;

#[allow(
    unused_imports,
    reason = "the direct-consumer interpreter will consume this complete retained seam"
)]
pub(crate) use decode::normalize_fetch_response;
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
    FetchBatch, FetchEndpoint, FetchHeader, FetchPartition, FetchRecord, FetchResponse,
    FetchTimestampType, FetchTopic,
};

#[cfg(test)]
mod batch_test;
#[cfg(test)]
mod decode_test;
#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod limits_test;
#[cfg(test)]
mod model_test;
