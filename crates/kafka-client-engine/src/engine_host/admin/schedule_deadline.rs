//! Deterministic minimum selection for concrete admin deadlines.

use kafka_client_core::Deadline;

pub(super) const fn earliest(left: Option<Deadline>, right: Option<Deadline>) -> Option<Deadline> {
    match (left, right) {
        (Some(left), Some(right)) if left.tick() <= right.tick() => Some(left),
        (Some(_left), Some(right)) => Some(right),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}
