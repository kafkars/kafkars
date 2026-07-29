//! Placement presence, signed lag, and exact broker-error scenarios.

use core::num::NonZeroI16;

use super::{DescribeReplicaLogDirsBrokerError, ReplicaLogDirInfo, ReplicaLogDirLocation};

#[test]
fn current_future_and_missing_placements_remain_distinct() {
    let current = ReplicaLogDirLocation::new("/logs/a".to_owned(), -1);
    let future = ReplicaLogDirLocation::new("/logs/b".to_owned(), 17);
    let info = ReplicaLogDirInfo::new(Some(current), Some(future));

    assert_eq!(
        info.current().map(ReplicaLogDirLocation::path),
        Some("/logs/a")
    );
    assert_eq!(
        info.current().map(ReplicaLogDirLocation::offset_lag),
        Some(-1)
    );
    assert_eq!(
        info.future().map(ReplicaLogDirLocation::path),
        Some("/logs/b")
    );
    assert_eq!(
        ReplicaLogDirInfo::new(None, None).into_parts(),
        (None, None)
    );
}

#[test]
fn broker_error_preserves_future_signed_code() {
    let error = DescribeReplicaLogDirsBrokerError::new(
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero")),
    );

    assert_eq!(error.code(), -32_000);
}
