//! Crate-private Fetch seam assembled without growing the declarative facade.

#[allow(
    unused_imports,
    reason = "the direct-consumer interpreter will consume this retained protocol seam"
)]
pub(crate) use super::{
    failure::FetchDecodeFailure,
    isolation::FetchIsolation,
    limits::FetchDecodeLimits,
    model::{
        FetchBatch, FetchEndpoint, FetchHeader, FetchPartition, FetchProducerIdentity, FetchRecord,
        FetchResponse, FetchTimestampType, FetchTopic,
    },
    outcome::{
        FetchBrokerFailure, FetchBrokerLevel, FetchOutcome, FetchOutcomeFailure,
        RejectedFetchOutcome, RetainedFetchOutcome,
    },
    outcome_failure::{FetchOutcomeFailureClass, classify_fetch_outcome_failure},
    outcome_normalize::{
        FetchSessionUpdate, fetch_session_requires_reestablishment, normalize_fetch_outcome,
        normalize_session_fetch_outcome,
    },
    request::{
        FETCH_NAME_ROUTE_MAX_VERSION, FETCH_NAME_ROUTE_MIN_VERSION, FetchRequestFailure,
        FetchRequestSettings, fetch_request, fetch_request_with_session,
    },
    request_broker::{
        BrokerFetchPartition, ForgottenFetchPartition, broker_fetch_request,
        fetch_session_close_request,
    },
    response::FetchResponseFailure,
    retention::{FetchOutputReservation, FetchReservationDomain, FetchRetentionFailure},
    session::FetchSessionRequest,
};
