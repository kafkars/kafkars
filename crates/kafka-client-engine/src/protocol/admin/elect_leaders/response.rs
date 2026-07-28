//! Bounded response validation and caller-order election correlation.

mod correlation;

use core::num::NonZeroI16;

use kafka_client_core::{ElectLeadersBatch, LeaderElectionBrokerError, LeaderElectionType};
use kafka_wire::ElectLeadersResponse;

use super::{
    LeaderElectionRef, ValidatedElectLeadersResponse, retention::result_charge,
    version::validate_selected_version,
};
use correlation::correlate_response;

const DIAGNOSTIC_LIMIT: usize = 1024;

/// Generated response facts unsafe to bind to the requested change set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ElectLeadersProtocolFailure {
    UnsupportedApiVersion {
        minimum: i16,
        maximum: i16,
        actual: i16,
    },
    NegativeThrottleTime,
    TopicCount,
    PartitionCount,
    UnexpectedTopic,
    MissingTopic,
    DuplicateTopic,
    UnexpectedPartition,
    MissingPartition,
    DuplicatePartition,
    NegativePartition,
    RetainedBytes,
}

/// Validates the selected version and generated response before owned copying.
pub(crate) fn validate_elect_leaders_response(
    election_type: LeaderElectionType,
    targets: &[LeaderElectionRef<'_>],
    response: &ElectLeadersResponse,
    selected_version: i16,
    result_limit: usize,
) -> Result<ValidatedElectLeadersResponse, ElectLeadersProtocolFailure> {
    validate_selected_version(selected_version, election_type).map_err(|failure| {
        ElectLeadersProtocolFailure::UnsupportedApiVersion {
            minimum: failure.minimum,
            maximum: failure.maximum,
            actual: failure.actual,
        }
    })?;
    let throttle_time_ms = u32::try_from(response.throttle_time_ms)
        .map_err(|_| ElectLeadersProtocolFailure::NegativeThrottleTime)?;
    if let Some(code) = NonZeroI16::new(response.error_code) {
        let error = bounded_error(code, None);
        ensure_top_level_limit(&error, result_limit)?;
        return Ok(ValidatedElectLeadersResponse::BrokerRejected(error));
    }
    let correlated = correlate_response(targets, response)?;
    let diagnostic_bytes = correlated.diagnostic_bytes()?;
    let charge = result_charge(targets.iter().copied(), diagnostic_bytes)
        .ok_or(ElectLeadersProtocolFailure::RetainedBytes)?;
    if charge > result_limit {
        return Err(ElectLeadersProtocolFailure::RetainedBytes);
    }
    let outcomes = correlated.normalize(targets)?;
    Ok(ValidatedElectLeadersResponse::Batch(
        ElectLeadersBatch::new(throttle_time_ms, outcomes),
    ))
}

fn ensure_top_level_limit(
    error: &LeaderElectionBrokerError,
    result_limit: usize,
) -> Result<(), ElectLeadersProtocolFailure> {
    let required = core::mem::size_of::<LeaderElectionBrokerError>()
        .checked_add(error.message().map_or(0, str::len))
        .ok_or(ElectLeadersProtocolFailure::RetainedBytes)?;
    (required <= result_limit)
        .then_some(())
        .ok_or(ElectLeadersProtocolFailure::RetainedBytes)
}

fn bounded_error(code: NonZeroI16, message: Option<&str>) -> LeaderElectionBrokerError {
    let (message, truncated) = match message {
        None => (None, false),
        Some(message) if message.len() <= DIAGNOSTIC_LIMIT => (Some(message.to_owned()), false),
        Some(message) => {
            let mut boundary = DIAGNOSTIC_LIMIT.min(message.len());
            while boundary > 0 && !message.is_char_boundary(boundary) {
                boundary -= 1;
            }
            (Some(message[..boundary].to_owned()), true)
        }
    };
    LeaderElectionBrokerError::with_bounded_message(code, message, truncated)
}
