//! Public current and future replica placement accessor coverage.

use super::{ReplicaLogDirInfo, ReplicaLogDirLocation};

#[test]
fn info_preserves_optional_current_and_future_placements() {
    let info = ReplicaLogDirInfo::new(
        Some(ReplicaLogDirLocation::new("/current".to_owned(), 0)),
        Some(ReplicaLogDirLocation::new("/future".to_owned(), 13)),
    );

    assert_eq!(
        info.current().map(ReplicaLogDirLocation::path),
        Some("/current")
    );
    assert_eq!(
        info.future().map(ReplicaLogDirLocation::offset_lag),
        Some(13)
    );
}

#[test]
fn info_represents_a_replica_missing_from_all_returned_directories() {
    let info = ReplicaLogDirInfo::new(None, None);

    assert!(info.current().is_none());
    assert!(info.future().is_none());
}
