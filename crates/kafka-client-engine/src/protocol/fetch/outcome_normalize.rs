//! Broker-first normalization of one raw, correlated Fetch response.

use core::num::NonZeroI16;

use kafka_wire::FetchResponse as WireFetchResponse;

use crate::protocol::consumer::throttle_ticks;

use super::{
    FetchBrokerLevel, FetchDecodeLimits, FetchIsolation, FetchOutcomeFailure,
    FetchOutputReservation, FetchSessionRequest, RejectedFetchOutcome, RetainedFetchOutcome,
    outcome::reject,
    outcome_retain::{retain_broker, retain_empty_success, retain_success},
    read_committed::filter_read_committed,
    response::{correlate_partition, normalize_correlated_response, validate_selected_version},
};

const FETCH_SESSION_MIN_VERSION: i16 = 7;
const FETCH_SESSION_ID_NOT_FOUND: i16 = 70;
const INVALID_FETCH_SESSION_EPOCH: i16 = 71;

/// Infallible state change reserved by a successfully normalized Fetch terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchSessionUpdate {
    Reset,
    Continue(FetchSessionRequest),
}

/// Reports whether one broker terminal invalidates an established session.
pub(crate) const fn fetch_session_requires_reestablishment(
    request: FetchSessionRequest,
    error_code: i16,
) -> bool {
    request.is_incremental()
        && matches!(
            error_code,
            FETCH_SESSION_ID_NOT_FOUND | INVALID_FETCH_SESSION_EPOCH
        )
}

/// Settles one raw no-session Fetch response under its admitted isolation.
///
/// Version and scalar broker failures are observed before success-only record
/// decoding. Read-committed filtering is response-local and preserves complete
/// batch progress after hiding aborted records. Temporary wire and decoded
/// storage is bounded independently by `limits`; `reservation` is the
/// pre-acquired hard ceiling for the final outcome and is settled before that
/// outcome can escape this function.
#[allow(
    clippy::too_many_arguments,
    reason = "the explicit terminal context prevents hidden correlation and budget authority"
)]
pub(crate) fn normalize_fetch_outcome(
    isolation: FetchIsolation,
    topic: &str,
    partition: u32,
    requested_offset: i64,
    selected_version: i16,
    response: WireFetchResponse,
    limits: FetchDecodeLimits,
    reservation: FetchOutputReservation,
) -> Result<RetainedFetchOutcome, RejectedFetchOutcome> {
    normalize_session_fetch_outcome(
        isolation,
        topic,
        None,
        partition,
        requested_offset,
        FetchSessionRequest::LEGACY,
        selected_version,
        response,
        limits,
        reservation,
    )
    .map(|(outcome, _session)| outcome)
}

/// Settles one raw Fetch response and returns its exact next session state.
#[allow(
    clippy::too_many_arguments,
    reason = "session correlation remains explicit beside the existing terminal context"
)]
pub(crate) fn normalize_session_fetch_outcome(
    isolation: FetchIsolation,
    topic: &str,
    topic_id: Option<[u8; 16]>,
    partition: u32,
    requested_offset: i64,
    session: FetchSessionRequest,
    selected_version: i16,
    response: WireFetchResponse,
    limits: FetchDecodeLimits,
    reservation: FetchOutputReservation,
) -> Result<(RetainedFetchOutcome, FetchSessionUpdate), RejectedFetchOutcome> {
    if let Err(failure) = validate_selected_version(selected_version) {
        return Err(reject(FetchOutcomeFailure::Response(failure), reservation));
    }
    if let Some(code) = NonZeroI16::new(response.error_code) {
        return retain_broker(FetchBrokerLevel::TopLevel, code, reservation)
            .map(|outcome| (outcome, FetchSessionUpdate::Reset));
    }
    if !(session.is_incremental() && response.responses.is_empty()) {
        let partition_code =
            match correlate_partition(topic, topic_id, partition, selected_version, &response) {
                Ok(partition) => NonZeroI16::new(partition.error_code),
                Err(failure) => {
                    return Err(reject(FetchOutcomeFailure::Response(failure), reservation));
                }
            };
        if let Some(code) = partition_code {
            return retain_broker(FetchBrokerLevel::Partition, code, reservation)
                .map(|outcome| (outcome, FetchSessionUpdate::Reset));
        }
    }
    let session_update = match session_update(session, selected_version, response.session_id) {
        Ok(update) => update,
        Err(failure) => return Err(reject(failure, reservation)),
    };
    if requested_offset < 0 {
        return Err(reject(
            FetchOutcomeFailure::InvalidRequestedOffset {
                actual: requested_offset,
            },
            reservation,
        ));
    }
    let Ok(throttle_time_ms) = u32::try_from(response.throttle_time_ms) else {
        return Err(reject(
            FetchOutcomeFailure::NegativeThrottleTime {
                actual: response.throttle_time_ms,
            },
            reservation,
        ));
    };
    let Some(throttle_ticks) = throttle_ticks(throttle_time_ms) else {
        return Err(reject(
            FetchOutcomeFailure::ThrottleTickOverflow {
                milliseconds: throttle_time_ms,
            },
            reservation,
        ));
    };
    if session.is_incremental() && response.responses.is_empty() {
        if let Err(failure) = normalize_correlated_response(response, limits) {
            return Err(reject(FetchOutcomeFailure::Response(failure), reservation));
        }
        return retain_empty_success(requested_offset, throttle_ticks, reservation)
            .map(|outcome| (outcome, session_update));
    }
    let mut normalized = match normalize_correlated_response(response, limits) {
        Ok(response) => response,
        Err(failure) => {
            return Err(reject(FetchOutcomeFailure::Response(failure), reservation));
        }
    };
    if isolation == FetchIsolation::ReadCommitted {
        if let Err(failure) = filter_read_committed(&mut normalized) {
            return Err(reject(
                FetchOutcomeFailure::Response(super::FetchResponseFailure::Decode(failure)),
                reservation,
            ));
        }
    }
    retain_success(requested_offset, throttle_ticks, normalized, reservation)
        .map(|outcome| (outcome, session_update))
}

pub(super) fn session_update(
    request: FetchSessionRequest,
    selected_version: i16,
    response_session_id: i32,
) -> Result<FetchSessionUpdate, FetchOutcomeFailure> {
    if response_session_id < 0 {
        return Err(FetchOutcomeFailure::UnexpectedSessionId {
            actual: response_session_id,
        });
    }
    if selected_version < FETCH_SESSION_MIN_VERSION || request.is_legacy() {
        return if response_session_id == 0 {
            Ok(FetchSessionUpdate::Reset)
        } else {
            Err(FetchOutcomeFailure::UnexpectedSessionId {
                actual: response_session_id,
            })
        };
    }
    if request.is_initial() {
        return if response_session_id == 0 {
            Ok(FetchSessionUpdate::Reset)
        } else {
            let metadata = FetchSessionRequest::incremental(response_session_id, 1).ok_or(
                FetchOutcomeFailure::UnexpectedSessionId {
                    actual: response_session_id,
                },
            )?;
            Ok(FetchSessionUpdate::Continue(metadata))
        };
    }
    if !request.is_incremental()
        || (response_session_id != 0 && response_session_id != request.session_id())
    {
        return Err(FetchOutcomeFailure::UnexpectedSessionId {
            actual: response_session_id,
        });
    }
    if response_session_id == 0 {
        return Ok(FetchSessionUpdate::Reset);
    }
    let next_epoch =
        request
            .next_incremental_epoch()
            .ok_or(FetchOutcomeFailure::UnexpectedSessionId {
                actual: response_session_id,
            })?;
    let metadata = FetchSessionRequest::incremental(response_session_id, next_epoch).ok_or(
        FetchOutcomeFailure::UnexpectedSessionId {
            actual: response_session_id,
        },
    )?;
    Ok(FetchSessionUpdate::Continue(metadata))
}
