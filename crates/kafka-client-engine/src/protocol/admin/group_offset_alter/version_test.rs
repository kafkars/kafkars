//! Exact `OffsetCommit` version-floor and ceiling scenarios.

use kafka_wire_core::ApiVersion;

use super::{
    OffsetCommitTargetRef, group_offset_alter_maximum_version, group_offset_alter_minimum_version,
    version::{GROUP_OFFSET_ALTER_MAX_VERSION, validate_selected_version},
};

#[test]
fn ordinary_alterations_have_an_exact_v2_floor_and_v9_ceiling() {
    let targets = [OffsetCommitTargetRef::new("orders", 0, 7, None, None)];
    assert_eq!(
        group_offset_alter_minimum_version(&targets),
        ApiVersion::new(2)
    );
    assert_eq!(GROUP_OFFSET_ALTER_MAX_VERSION, ApiVersion::new(9));
    assert_eq!(group_offset_alter_maximum_version(None), ApiVersion::new(9));
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

#[test]
fn explicit_retention_caps_the_ceiling_at_v4() {
    let targets = [OffsetCommitTargetRef::new("orders", 0, 7, None, None)];

    assert_eq!(
        group_offset_alter_minimum_version(&targets),
        ApiVersion::new(2)
    );
    assert_eq!(
        group_offset_alter_maximum_version(Some(86_400_000)),
        ApiVersion::new(4)
    );
    assert!(validate_selected_version(&targets, Some(86_400_000), 4).is_ok());
    assert!(validate_selected_version(&targets, Some(86_400_000), 5).is_err());
}

#[test]
fn retention_and_leader_epoch_have_no_selectable_shared_version() {
    let targets = [OffsetCommitTargetRef::new("orders", 0, 7, Some(3), None)];

    assert_eq!(
        group_offset_alter_minimum_version(&targets),
        ApiVersion::new(6)
    );
    assert_eq!(
        group_offset_alter_maximum_version(Some(86_400_000)),
        ApiVersion::new(4)
    );
    for actual in 2..=9 {
        assert!(validate_selected_version(&targets, Some(86_400_000), actual).is_err());
    }
}
