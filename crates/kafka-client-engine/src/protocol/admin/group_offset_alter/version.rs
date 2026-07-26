//! Exact `OffsetCommit` version policy derived from requested semantic fields.

use kafka_wire_core::ApiVersion;

use super::OffsetCommitTargetRef;

const BASE_MIN_VERSION: ApiVersion = ApiVersion::new(2);
const LEADER_EPOCH_MIN_VERSION: ApiVersion = ApiVersion::new(6);
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

pub(super) fn validate_selected_version(
    targets: &[OffsetCommitTargetRef<'_>],
    actual: i16,
) -> Result<(), SelectedVersionFailure> {
    let minimum = group_offset_alter_minimum_version(targets).value();
    if actual < minimum || actual > GROUP_OFFSET_ALTER_MAX_VERSION.value() {
        return Err(SelectedVersionFailure {
            minimum,
            maximum: GROUP_OFFSET_ALTER_MAX_VERSION.value(),
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
