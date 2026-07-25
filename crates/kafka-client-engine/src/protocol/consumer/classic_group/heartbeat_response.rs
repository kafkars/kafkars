//! Generated `Heartbeat` response normalization without membership policy.

use core::num::NonZeroI16;

use kafka_wire::HeartbeatResponse;

use super::{
    ClassicBrokerRejection,
    validation::{HEARTBEAT_MAX_VERSION, HEARTBEAT_MIN_VERSION},
};

/// One exact Heartbeat terminal without retry or membership policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicHeartbeatOutcome {
    Succeeded { throttle_time_ms: u32 },
    Rejected(ClassicBrokerRejection),
}

/// Generated response facts that cannot safely enter deterministic policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicHeartbeatResponseFailure {
    UnsupportedApiVersion(i16),
    UnexpectedThrottleTime(i32),
    NegativeThrottleTime(i32),
}

/// Normalizes one selected v0-v2 Heartbeat terminal without classifying errors.
pub(crate) fn normalize_classic_heartbeat_response(
    selected_version: i16,
    response: &HeartbeatResponse,
) -> Result<ClassicHeartbeatOutcome, ClassicHeartbeatResponseFailure> {
    if !(HEARTBEAT_MIN_VERSION..=HEARTBEAT_MAX_VERSION).contains(&selected_version) {
        return Err(ClassicHeartbeatResponseFailure::UnsupportedApiVersion(
            selected_version,
        ));
    }
    let throttle_time_ms = normalize_throttle(selected_version, response.throttle_time_ms)?;
    Ok(match NonZeroI16::new(response.error_code) {
        Some(error_code) => ClassicHeartbeatOutcome::Rejected(ClassicBrokerRejection::new(
            throttle_time_ms,
            error_code,
        )),
        None => ClassicHeartbeatOutcome::Succeeded { throttle_time_ms },
    })
}

fn normalize_throttle(
    version: i16,
    throttle_time_ms: i32,
) -> Result<u32, ClassicHeartbeatResponseFailure> {
    if version == 0 && throttle_time_ms != 0 {
        return Err(ClassicHeartbeatResponseFailure::UnexpectedThrottleTime(
            throttle_time_ms,
        ));
    }
    u32::try_from(throttle_time_ms)
        .map_err(|_error| ClassicHeartbeatResponseFailure::NegativeThrottleTime(throttle_time_ms))
}
