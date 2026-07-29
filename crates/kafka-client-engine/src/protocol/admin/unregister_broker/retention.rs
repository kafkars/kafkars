//! Small explicit terminal and UTF-8 diagnostic retention bounds.

use core::mem::size_of;

use super::NormalizedUnregisterBrokerResponse;

pub(crate) const UNREGISTER_BROKER_MAX_RETAINED_BYTES: usize = 4 * 1_024;
pub(super) const UNREGISTER_BROKER_MAX_DIAGNOSTIC_BYTES: usize = 1_024;

pub(super) fn bounded_diagnostic(source: Option<&str>) -> (Option<&str>, bool) {
    let Some(source) = source else {
        return (None, false);
    };
    let mut end = source.len().min(UNREGISTER_BROKER_MAX_DIAGNOSTIC_BYTES);
    while !source.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    (Some(&source[..end]), end < source.len())
}

pub(super) fn retained_charge(diagnostic_capacity: usize) -> Option<usize> {
    size_of::<NormalizedUnregisterBrokerResponse>().checked_add(diagnostic_capacity)
}
