//! Control-only normalization for one forgotten-partition Fetch-session epoch.

use core::num::NonZeroI16;

use kafka_wire::FetchResponse as WireFetchResponse;

use crate::protocol::consumer::throttle_ticks;

use super::{
    FetchBrokerFailure, FetchBrokerLevel, FetchOutcomeFailure, FetchResponseFailure,
    FetchSessionRequest, FetchSessionUpdate, outcome_normalize::session_update,
    response::validate_selected_version,
};

const FETCH_SESSION_MIN_VERSION: i16 = 7;

/// A broker failure or successful session advance from one control-only Fetch.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ForgottenFetchOutcome {
    BrokerFailure(FetchBrokerFailure),
    Success {
        throttle_ticks: Option<u64>,
        session: FetchSessionUpdate,
    },
}

impl ForgottenFetchOutcome {
    pub(crate) const fn broker_failure(&self) -> Option<FetchBrokerFailure> {
        match self {
            Self::BrokerFailure(failure) => Some(*failure),
            Self::Success { .. } => None,
        }
    }

    pub(crate) const fn session(&self) -> Option<FetchSessionUpdate> {
        match self {
            Self::BrokerFailure(_) => None,
            Self::Success { session, .. } => Some(*session),
        }
    }

    pub(crate) const fn throttle_ticks(&self) -> Option<u64> {
        match self {
            Self::BrokerFailure(_) => None,
            Self::Success { throttle_ticks, .. } => *throttle_ticks,
        }
    }
}

/// Normalizes one incremental response without inventing partition ownership.
#[allow(
    clippy::needless_pass_by_value,
    reason = "normalization consumes the exact terminal response even when all retained fields are scalar"
)]
pub(crate) fn normalize_forgotten_fetch_outcome(
    session: FetchSessionRequest,
    selected_version: i16,
    response: WireFetchResponse,
) -> Result<ForgottenFetchOutcome, FetchOutcomeFailure> {
    validate_selected_version(selected_version).map_err(FetchOutcomeFailure::Response)?;
    if selected_version < FETCH_SESSION_MIN_VERSION {
        return Err(FetchOutcomeFailure::Response(
            FetchResponseFailure::UnsupportedApiVersion {
                actual: selected_version,
            },
        ));
    }
    if let Some(code) = NonZeroI16::new(response.error_code) {
        return Ok(ForgottenFetchOutcome::BrokerFailure(
            FetchBrokerFailure::new(FetchBrokerLevel::TopLevel, code, None),
        ));
    }
    if !response.responses.is_empty() {
        return Err(FetchOutcomeFailure::Response(
            FetchResponseFailure::TopicCount {
                actual: response.responses.len(),
            },
        ));
    }
    let session = session_update(session, selected_version, response.session_id)?;
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_error| {
        FetchOutcomeFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let throttle_ticks =
        throttle_ticks(throttle_time_ms).ok_or(FetchOutcomeFailure::ThrottleTickOverflow {
            milliseconds: throttle_time_ms,
        })?;
    Ok(ForgottenFetchOutcome::Success {
        throttle_ticks: Some(throttle_ticks),
        session,
    })
}
