//! Core-owned Kafka API-version floors for Admin `ListOffsets` semantics.

use kafka_client_core::{AdminListOffsetTarget, ReadIsolation};

/// Returns the earliest API-key 2 version representing both requested policies.
pub(crate) const fn minimum_api_version(
    target: &AdminListOffsetTarget,
    read_isolation: ReadIsolation,
) -> i16 {
    target.minimum_api_version(read_isolation)
}
