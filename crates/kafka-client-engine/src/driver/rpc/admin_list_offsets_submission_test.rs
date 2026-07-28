//! Admin `ListOffsets` leader route and exact version-window scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{AdminListOffsetSpec, AdminListOffsetTarget, ReadIsolation};
use kafka_driver::{ApiVersion, TrafficClass};

use super::admin_list_offsets_submission::admin_list_offsets_options;

#[test]
fn options_preserve_deadline_lane_and_exact_selector_version_windows() {
    let deadline = Instant::now() + Duration::from_secs(7);
    for (spec, expected_minimum) in [
        (AdminListOffsetSpec::Earliest, 1),
        (AdminListOffsetSpec::Latest, 1),
        (AdminListOffsetSpec::Timestamp(123), 1),
        (AdminListOffsetSpec::MaxTimestamp, 7),
        (AdminListOffsetSpec::EarliestLocal, 8),
        (AdminListOffsetSpec::LatestTiered, 9),
        (AdminListOffsetSpec::EarliestPendingUpload, 11),
    ] {
        let target = target(spec);
        let options = admin_list_offsets_options(&target, ReadIsolation::ReadUncommitted, deadline);

        assert_eq!(options.deadline(), deadline);
        assert_eq!(options.traffic_class(), TrafficClass::Interactive);
        assert_eq!(
            options.minimum_version(),
            Some(ApiVersion::new(expected_minimum))
        );
        assert_eq!(options.maximum_version(), Some(ApiVersion::new(11)));
    }
}

#[test]
fn read_committed_raises_only_the_legacy_selector_floor_to_v2() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let legacy_target = target(AdminListOffsetSpec::Latest);
    let legacy = admin_list_offsets_options(&legacy_target, ReadIsolation::ReadCommitted, deadline);
    let tiered_target = target(AdminListOffsetSpec::LatestTiered);
    let tiered = admin_list_offsets_options(&tiered_target, ReadIsolation::ReadCommitted, deadline);

    assert_eq!(legacy.minimum_version(), Some(ApiVersion::new(2)));
    assert_eq!(tiered.minimum_version(), Some(ApiVersion::new(9)));
}

#[test]
fn current_leader_epoch_raises_only_the_legacy_selector_floor_to_v4() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let fenced = target(AdminListOffsetSpec::Latest).with_current_leader_epoch(Some(19));
    let already_newer =
        target(AdminListOffsetSpec::MaxTimestamp).with_current_leader_epoch(Some(19));

    assert_eq!(
        admin_list_offsets_options(&fenced, ReadIsolation::ReadUncommitted, deadline)
            .minimum_version(),
        Some(ApiVersion::new(4))
    );
    assert_eq!(
        admin_list_offsets_options(&already_newer, ReadIsolation::ReadUncommitted, deadline)
            .minimum_version(),
        Some(ApiVersion::new(7))
    );
}

fn target(spec: AdminListOffsetSpec) -> AdminListOffsetTarget {
    AdminListOffsetTarget::new("audit".to_owned(), 3, spec)
}
