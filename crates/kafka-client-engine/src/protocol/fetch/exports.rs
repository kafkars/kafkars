//! Crate-private Fetch seam assembled without growing the declarative facade.

#[allow(
    unused_imports,
    reason = "the direct-consumer interpreter will consume this retained protocol seam"
)]
pub(crate) use super::{
    failure::FetchDecodeFailure,
    limits::FetchDecodeLimits,
    model::{
        FetchBatch, FetchEndpoint, FetchHeader, FetchPartition, FetchProducerIdentity, FetchRecord,
        FetchResponse, FetchTimestampType, FetchTopic,
    },
    outcome::{
        FetchBrokerFailure, FetchBrokerLevel, FetchOutcome, FetchOutcomeFailure,
        RejectedFetchOutcome, RetainedFetchOutcome,
    },
    outcome_normalize::normalize_read_uncommitted_fetch_outcome,
    request::{
        FETCH_NAME_ROUTE_MAX_VERSION, FETCH_NAME_ROUTE_MIN_VERSION, FetchRequestFailure,
        FetchRequestSettings, fetch_request,
    },
    response::FetchResponseFailure,
    retention::{FetchOutputReservation, FetchRetentionFailure},
};
