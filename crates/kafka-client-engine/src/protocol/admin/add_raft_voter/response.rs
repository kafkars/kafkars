//! Validate-first normalization of generated `AddRaftVoter` responses.

use kafka_wire::AddRaftVoterResponse;

use super::{
    NormalizedAddRaftVoterResponse,
    retention::{ADD_RAFT_VOTER_MAX_RETAINED_BYTES, bounded_diagnostic, retained_charge},
};

/// Incompatible, malformed, allocation-failed, or over-capacity response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddRaftVoterResponseFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    RetainedBytes { required: usize, limit: usize },
    Allocation { requested: usize },
}

/// Preserves the exact signed status and one bounded nullable UTF-8 diagnostic.
pub(crate) fn normalize_add_raft_voter_response(
    selected_version: Option<i16>,
    response: &AddRaftVoterResponse,
    retained_limit: usize,
) -> Result<NormalizedAddRaftVoterResponse, AddRaftVoterResponseFailure> {
    let selected_version =
        selected_version.ok_or(AddRaftVoterResponseFailure::MissingSelectedVersion)?;
    if !(0..=1).contains(&selected_version) {
        return Err(AddRaftVoterResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        AddRaftVoterResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let effective_limit = retained_limit.min(ADD_RAFT_VOTER_MAX_RETAINED_BYTES);
    let (bounded, diagnostic_truncated) = bounded_diagnostic(response.error_message.as_deref());
    let projected = retained_charge(bounded.map_or(0, str::len)).unwrap_or(usize::MAX);
    ensure_limit(projected, effective_limit)?;
    let diagnostic = bounded
        .map(|source| {
            let mut owned = String::new();
            owned.try_reserve_exact(source.len()).map_err(|_| {
                AddRaftVoterResponseFailure::Allocation {
                    requested: source.len(),
                }
            })?;
            owned.push_str(source);
            Ok(owned)
        })
        .transpose()?;
    let retained =
        retained_charge(diagnostic.as_ref().map_or(0, String::capacity)).unwrap_or(usize::MAX);
    ensure_limit(retained, effective_limit)?;
    Ok(NormalizedAddRaftVoterResponse::new(
        throttle_time_ms,
        response.error_code,
        diagnostic,
        diagnostic_truncated,
        retained,
    ))
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), AddRaftVoterResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(AddRaftVoterResponseFailure::RetainedBytes { required, limit })
}
