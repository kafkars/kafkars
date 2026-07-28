//! Lossless scalar and version-presence scenarios for log-directory outcomes.

use core::num::NonZeroI16;

use super::{
    AdminDescribeLogDirsBrokerError, AdminLogDirDescription, AdminLogDirOutcome,
    AdminLogDirReplicaInfo, AdminLogDirResult,
};

#[test]
fn description_retains_replica_and_newer_version_volume_facts() {
    let description = AdminLogDirDescription::new(
        vec![AdminLogDirReplicaInfo::new(
            "orders".to_owned(),
            2,
            4_096,
            7,
            true,
        )],
        Some(1_000_000),
        Some(700_000),
        Some(true),
    );

    assert_eq!(description.replicas()[0].topic(), "orders");
    assert_eq!(description.replicas()[0].partition(), 2);
    assert_eq!(description.replicas()[0].size_bytes(), 4_096);
    assert_eq!(description.replicas()[0].offset_lag(), 7);
    assert!(description.replicas()[0].is_future());
    assert_eq!(description.total_bytes(), Some(1_000_000));
    assert_eq!(description.usable_bytes(), Some(700_000));
    assert_eq!(description.cordoned(), Some(true));
}

#[test]
fn older_version_absence_and_exact_signed_directory_error_are_distinct() {
    let description = AdminLogDirDescription::new(Vec::new(), None, None, None);
    assert_eq!(description.total_bytes(), None);
    assert_eq!(description.usable_bytes(), None);
    assert_eq!(description.cordoned(), None);

    let error = AdminDescribeLogDirsBrokerError::new(
        NonZeroI16::new(-17).unwrap_or_else(|| panic!("nonzero")),
    );
    let outcome = AdminLogDirOutcome::broker_failed("/logs/a".to_owned(), error);
    assert_eq!(outcome.path(), "/logs/a");
    let AdminLogDirResult::BrokerFailed(error) = outcome.result() else {
        panic!("expected exact directory failure");
    };
    assert_eq!(error.code(), -17);
}
