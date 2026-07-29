//! Fallible retained-capacity accounting and bounded diagnostic copying.

use core::mem::size_of;

use super::{
    DescribeProducersProtocolFailure, NormalizedDescribeProducersResponse, NormalizedProducerState,
    model::DESCRIBE_PRODUCERS_DIAGNOSTIC_BYTES,
};

pub(super) const fn states_charge(capacity: usize) -> Option<usize> {
    match capacity.checked_mul(size_of::<NormalizedProducerState>()) {
        Some(states) => size_of::<NormalizedDescribeProducersResponse>().checked_add(states),
        None => None,
    }
}

pub(super) const fn diagnostic_charge(capacity: usize) -> Option<usize> {
    size_of::<NormalizedDescribeProducersResponse>().checked_add(capacity)
}

pub(super) fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), DescribeProducersProtocolFailure> {
    if required > limit {
        return Err(DescribeProducersProtocolFailure::RetainedBytes { required, limit });
    }
    Ok(())
}

pub(super) fn retained_diagnostic(
    source: &str,
    limit: usize,
) -> Result<(String, bool, usize), DescribeProducersProtocolFailure> {
    let (prefix, truncated) = bounded_diagnostic(source);
    let minimum = diagnostic_charge(prefix.len()).unwrap_or(usize::MAX);
    ensure_limit(minimum, limit)?;

    let mut message = String::new();
    message.try_reserve_exact(prefix.len()).map_err(|_| {
        DescribeProducersProtocolFailure::Allocation {
            field: "error_message",
            requested: prefix.len(),
        }
    })?;
    message.push_str(prefix);
    let retained = diagnostic_charge(message.capacity()).unwrap_or(usize::MAX);
    ensure_limit(retained, limit)?;
    Ok((message, truncated, retained))
}

fn bounded_diagnostic(source: &str) -> (&str, bool) {
    if source.len() <= DESCRIBE_PRODUCERS_DIAGNOSTIC_BYTES {
        return (source, false);
    }
    let mut end = DESCRIBE_PRODUCERS_DIAGNOSTIC_BYTES;
    while !source.is_char_boundary(end) {
        end -= 1;
    }
    (&source[..end], true)
}
