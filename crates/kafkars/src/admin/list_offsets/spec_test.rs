//! Public Admin `OffsetSpec` construction scenarios.

use super::OffsetSpec;

#[test]
fn constructors_preserve_each_closed_policy() {
    assert_eq!(OffsetSpec::earliest(), OffsetSpec::Earliest);
    assert_eq!(OffsetSpec::latest(), OffsetSpec::Latest);
    assert_eq!(OffsetSpec::max_timestamp(), OffsetSpec::MaxTimestamp);
    assert_eq!(OffsetSpec::earliest_local(), OffsetSpec::EarliestLocal);
    assert_eq!(OffsetSpec::latest_tiered(), OffsetSpec::LatestTiered);
    assert_eq!(
        OffsetSpec::earliest_pending_upload(),
        OffsetSpec::EarliestPendingUpload
    );
    assert_eq!(
        OffsetSpec::for_timestamp(1_700_000_000_123),
        OffsetSpec::Timestamp(1_700_000_000_123)
    );
}
