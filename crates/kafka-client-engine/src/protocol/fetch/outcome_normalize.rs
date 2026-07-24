//! Broker-first normalization of one raw, correlated Fetch response.

use core::num::NonZeroI16;

use kafka_wire::FetchResponse as WireFetchResponse;

use crate::protocol::consumer::throttle_ticks;

use super::{
    FetchBrokerLevel, FetchDecodeLimits, FetchOutcomeFailure, FetchOutputReservation,
    RejectedFetchOutcome, RetainedFetchOutcome,
    outcome::reject,
    outcome_retain::{retain_broker, retain_success},
    response::{correlate_partition, normalize_correlated_response, validate_selected_version},
};

/// Settles one raw no-session, read-uncommitted Fetch response.
///
/// Version and scalar broker failures are observed before success-only record
/// decoding. Aborted-transaction markers remain uninterpreted; read-committed
/// execution requires a separate filtering policy. Temporary wire and decoded
/// storage is bounded independently by `limits`; `reservation` is the
/// pre-acquired hard ceiling for the final outcome and is settled before that
/// outcome can escape this function.
#[allow(
    clippy::too_many_arguments,
    reason = "the explicit terminal context prevents hidden correlation and budget authority"
)]
pub(crate) fn normalize_read_uncommitted_fetch_outcome(
    topic: &str,
    partition: u32,
    requested_offset: i64,
    selected_version: i16,
    response: WireFetchResponse,
    limits: FetchDecodeLimits,
    reservation: FetchOutputReservation,
) -> Result<RetainedFetchOutcome, RejectedFetchOutcome> {
    if let Err(failure) = validate_selected_version(selected_version) {
        return Err(reject(FetchOutcomeFailure::Response(failure), reservation));
    }
    if let Some(code) = NonZeroI16::new(response.error_code) {
        return retain_broker(FetchBrokerLevel::TopLevel, code, reservation);
    }
    let partition_code = match correlate_partition(topic, partition, &response) {
        Ok(partition) => NonZeroI16::new(partition.error_code),
        Err(failure) => {
            return Err(reject(FetchOutcomeFailure::Response(failure), reservation));
        }
    };
    if let Some(code) = partition_code {
        return retain_broker(FetchBrokerLevel::Partition, code, reservation);
    }
    if requested_offset < 0 {
        return Err(reject(
            FetchOutcomeFailure::InvalidRequestedOffset {
                actual: requested_offset,
            },
            reservation,
        ));
    }
    if response.session_id != 0 {
        return Err(reject(
            FetchOutcomeFailure::UnexpectedSessionId {
                actual: response.session_id,
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
    let normalized = match normalize_correlated_response(response, limits) {
        Ok(response) => response,
        Err(failure) => {
            return Err(reject(FetchOutcomeFailure::Response(failure), reservation));
        }
    };
    retain_success(requested_offset, throttle_ticks, normalized, reservation)
}
