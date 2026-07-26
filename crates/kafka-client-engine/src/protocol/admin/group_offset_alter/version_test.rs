//! Exact `OffsetCommit` version-floor and ceiling scenarios.

use kafka_wire_core::ApiVersion;

use super::{
    OffsetCommitTargetRef,
    version::{GROUP_OFFSET_ALTER_MAX_VERSION, group_offset_alter_minimum_version},
};

#[test]
fn ordinary_alterations_have_an_exact_v2_floor_and_v9_ceiling() {
    let targets = [OffsetCommitTargetRef::new("orders", 0, 7, None, None)];
    assert_eq!(
        group_offset_alter_minimum_version(&targets),
        ApiVersion::new(2)
    );
    assert_eq!(GROUP_OFFSET_ALTER_MAX_VERSION, ApiVersion::new(9));
}

#[test]
fn one_present_leader_epoch_raises_the_floor_to_v6() {
    let targets = [
        OffsetCommitTargetRef::new("orders", 0, 7, None, None),
        OffsetCommitTargetRef::new("audit", 1, 9, Some(3), None),
    ];
    assert_eq!(
        group_offset_alter_minimum_version(&targets),
        ApiVersion::new(6)
    );
}
