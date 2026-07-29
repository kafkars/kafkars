//! Exact `OffsetCommit` version policy derived from requested semantic fields.

use kafka_wire_core::ApiVersion;

use super::OffsetCommitTargetRef;

const BASE_MIN_VERSION: ApiVersion = ApiVersion::new(2);
const LEADER_EPOCH_MIN_VERSION: ApiVersion = ApiVersion::new(6);
const RETENTION_MAX_VERSION: ApiVersion = ApiVersion::new(4);
pub(crate) const GROUP_OFFSET_ALTER_MAX_VERSION: ApiVersion = ApiVersion::new(9);

/// Returns v6 only when a present leader epoch requires that field.
pub(crate) fn group_offset_alter_minimum_version(
    targets: &[OffsetCommitTargetRef<'_>],
) -> ApiVersion {
    if targets.iter().any(|target| target.leader_epoch().is_some()) {
        LEADER_EPOCH_MIN_VERSION
    } else {
        BASE_MIN_VERSION
    }
}

/// Caps negotiation at v4 only when Kafka must carry explicit retention.
pub(crate) const fn group_offset_alter_maximum_version(
    retention_time_ms: Option<i64>,
) -> ApiVersion {
    if retention_time_ms.is_some() {
        RETENTION_MAX_VERSION
    } else {
        GROUP_OFFSET_ALTER_MAX_VERSION
    }
}

pub(super) fn validate_selected_version(
    targets: &[OffsetCommitTargetRef<'_>],
    retention_time_ms: Option<i64>,
    actual: i16,
) -> Result<(), SelectedVersionFailure> {
    let minimum = group_offset_alter_minimum_version(targets).value();
    let maximum = group_offset_alter_maximum_version(retention_time_ms).value();
    if actual < minimum || actual > maximum {
        return Err(SelectedVersionFailure {
            minimum,
            maximum,
            actual,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SelectedVersionFailure {
    pub(super) minimum: i16,
    pub(super) maximum: i16,
    pub(super) actual: i16,
}
