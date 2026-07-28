//! Generated `LeaveGroup` response normalization without recovery policy.

use core::num::NonZeroI16;

use kafka_wire::LeaveGroupResponse;

use super::validation::{LEAVE_MAX_VERSION, LEAVE_MIN_VERSION, STATIC_LEAVE_VERSION};

/// One exact classic-member `LeaveGroup` broker terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicLeaveGroupOutcome {
    Succeeded {
        throttle_time_ms: u32,
    },
    Rejected {
        throttle_time_ms: u32,
        error_code: NonZeroI16,
    },
}

/// Generated response facts that cannot safely enter close settlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicLeaveGroupResponseFailure {
    UnsupportedApiVersion(i16),
    UnexpectedThrottleTime(i32),
    NegativeThrottleTime(i32),
    UnexpectedMembers,
}

/// Normalizes one selected dynamic v0-v2 or static v3 terminal.
pub(crate) fn normalize_classic_leave_group_response(
    selected_version: i16,
    response: &LeaveGroupResponse,
) -> Result<ClassicLeaveGroupOutcome, ClassicLeaveGroupResponseFailure> {
    if !(LEAVE_MIN_VERSION..=LEAVE_MAX_VERSION).contains(&selected_version)
        && selected_version != STATIC_LEAVE_VERSION
    {
        return Err(ClassicLeaveGroupResponseFailure::UnsupportedApiVersion(
            selected_version,
        ));
    }
    if selected_version == STATIC_LEAVE_VERSION {
        if response.members.len() != 1 {
            return Err(ClassicLeaveGroupResponseFailure::UnexpectedMembers);
        }
    } else if !response.members.is_empty() {
        return Err(ClassicLeaveGroupResponseFailure::UnexpectedMembers);
    }

    let throttle_time_ms = normalize_throttle(selected_version, response.throttle_time_ms)?;
    let error_code = if selected_version == STATIC_LEAVE_VERSION && response.error_code == 0 {
        response.members[0].error_code
    } else {
        response.error_code
    };
    Ok(match NonZeroI16::new(error_code) {
        Some(error_code) => ClassicLeaveGroupOutcome::Rejected {
            throttle_time_ms,
            error_code,
        },
        None => ClassicLeaveGroupOutcome::Succeeded { throttle_time_ms },
    })
}

fn normalize_throttle(
    version: i16,
    throttle_time_ms: i32,
) -> Result<u32, ClassicLeaveGroupResponseFailure> {
    if version == 0 && throttle_time_ms != 0 {
        return Err(ClassicLeaveGroupResponseFailure::UnexpectedThrottleTime(
            throttle_time_ms,
        ));
    }
    u32::try_from(throttle_time_ms)
        .map_err(|_error| ClassicLeaveGroupResponseFailure::NegativeThrottleTime(throttle_time_ms))
}
