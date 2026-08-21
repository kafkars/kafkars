//! Stable version presence and exact replica scalar tests.

use super::{LogDirDescription, LogDirReplica};

#[test]
fn versioned_volume_facts_and_replica_scalars_remain_exact() {
    let replica = LogDirReplica::new("orders".to_owned(), 3, 8_192, -7, true);
    let description =
        LogDirDescription::new(Some(1_000_000), Some(750_000), Some(false), vec![replica]);

    assert_eq!(description.total_bytes(), Some(1_000_000));
    assert_eq!(description.usable_bytes(), Some(750_000));
    assert_eq!(description.is_cordoned(), Some(false));
    let replica = &description.replicas()[0];
    assert_eq!(replica.topic_name(), "orders");
    assert_eq!(replica.partition_index(), 3);
    assert_eq!(replica.partition_size(), 8_192);
    assert_eq!(replica.offset_lag(), -7);
    assert!(replica.is_future());
}

#[test]
fn older_version_volume_and_cordon_absence_remains_distinct() {
    let description = LogDirDescription::new(None, None, None, Vec::new());

    assert_eq!(description.total_bytes(), None);
    assert_eq!(description.usable_bytes(), None);
    assert_eq!(description.is_cordoned(), None);
    assert!(description.into_replicas().is_empty());
}
