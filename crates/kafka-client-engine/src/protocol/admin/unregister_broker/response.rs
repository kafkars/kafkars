//! Validate-first normalization of the sole generated v0 response.

use kafka_wire::UnregisterBrokerResponse;

use super::{
    NormalizedUnregisterBrokerResponse,
    retention::{UNREGISTER_BROKER_MAX_RETAINED_BYTES, bounded_diagnostic, retained_charge},
};

/// Incompatible, malformed, allocation-failed, or over-capacity response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnregisterBrokerResponseFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    RetainedBytes { required: usize, limit: usize },
    Allocation { requested: usize },
}

/// Preserves exact status and one bounded nullable UTF-8 diagnostic.
pub(crate) fn normalize_unregister_broker_response(
    selected_version: Option<i16>,
    response: &UnregisterBrokerResponse,
    retained_limit: usize,
) -> Result<NormalizedUnregisterBrokerResponse, UnregisterBrokerResponseFailure> {
    let selected_version =
        selected_version.ok_or(UnregisterBrokerResponseFailure::MissingSelectedVersion)?;
    if selected_version != 0 {
        return Err(UnregisterBrokerResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        UnregisterBrokerResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let effective_limit = retained_limit.min(UNREGISTER_BROKER_MAX_RETAINED_BYTES);
    let (bounded, diagnostic_truncated) = bounded_diagnostic(response.error_message.as_deref());
    let projected = retained_charge(bounded.map_or(0, str::len)).unwrap_or(usize::MAX);
    ensure_limit(projected, effective_limit)?;
    let diagnostic = bounded
        .map(|source| {
            let mut owned = String::new();
            owned.try_reserve_exact(source.len()).map_err(|_| {
                UnregisterBrokerResponseFailure::Allocation {
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
    Ok(NormalizedUnregisterBrokerResponse::new(
        throttle_time_ms,
        response.error_code,
        diagnostic,
        diagnostic_truncated,
        retained,
    ))
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), UnregisterBrokerResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(UnregisterBrokerResponseFailure::RetainedBytes { required, limit })
}
