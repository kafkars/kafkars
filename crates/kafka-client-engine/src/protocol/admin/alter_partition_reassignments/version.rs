//! Exact generated version window for `AlterPartitionReassignments`.

use kafka_wire_core::ApiVersion;

pub(crate) const ALTER_PARTITION_REASSIGNMENTS_MIN_VERSION: ApiVersion = ApiVersion::new(0);
pub(crate) const ALTER_PARTITION_REASSIGNMENTS_MAX_VERSION: ApiVersion = ApiVersion::new(1);

pub(crate) const fn minimum_version_for_policy(
    allow_replication_factor_change: bool,
) -> ApiVersion {
    if allow_replication_factor_change {
        ALTER_PARTITION_REASSIGNMENTS_MIN_VERSION
    } else {
        ApiVersion::new(1)
    }
}

pub(super) fn validate_selected_version(
    actual: i16,
    allow_replication_factor_change: bool,
) -> Result<(), SelectedVersionFailure> {
    let minimum = minimum_version_for_policy(allow_replication_factor_change).value();
    let maximum = ALTER_PARTITION_REASSIGNMENTS_MAX_VERSION.value();
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
