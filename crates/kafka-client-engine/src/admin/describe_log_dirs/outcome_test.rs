//! Lossless core-to-engine log-directory result translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminDescribeLogDirsBatch, AdminDescribeLogDirsBrokerError, AdminDescribeLogDirsBrokerOutcome,
    AdminDescribeLogDirsTerminal, AdminLogDirDescription, AdminLogDirOutcome,
    AdminLogDirReplicaInfo,
};

use super::{
    DescribeLogDirsEngineBrokerResult, DescribeLogDirsOutcome, outcome::translate_terminal,
};

#[test]
fn translation_retains_nested_codes_version_presence_and_replica_scalars() {
    let error = AdminDescribeLogDirsBrokerError::new(
        NonZeroI16::new(56).unwrap_or_else(|| panic!("nonzero")),
    );
    let directory = AdminLogDirOutcome::described(
        "/data".to_owned(),
        AdminLogDirDescription::new(
            vec![AdminLogDirReplicaInfo::new(
                "orders".to_owned(),
                3,
                4096,
                -1,
                true,
            )],
            Some(100),
            Some(75),
            Some(false),
        ),
    );
    let terminal = AdminDescribeLogDirsTerminal::Described(AdminDescribeLogDirsBatch::new(
        9,
        vec![
            AdminDescribeLogDirsBrokerOutcome::described(7, vec![directory]),
            AdminDescribeLogDirsBrokerOutcome::broker_failed(2, error),
        ],
    ));

    let DescribeLogDirsOutcome::Described(batch) = translate_terminal(terminal) else {
        panic!("expected described batch");
    };
    let (throttle, outcomes) = batch.into_parts();
    assert_eq!(throttle, 9);
    let (broker, result) = outcomes
        .into_iter()
        .next()
        .expect("first broker")
        .into_parts();
    assert_eq!(broker, 7);
    let DescribeLogDirsEngineBrokerResult::Described(log_dirs) = result else {
        panic!("expected directories");
    };
    let (path, description) = log_dirs.into_iter().next().expect("directory").into_parts();
    assert_eq!(path, "/data");
    let (replicas, total, usable, cordoned) =
        description.expect("successful directory").into_parts();
    assert_eq!(
        (total, usable, cordoned),
        (Some(100), Some(75), Some(false))
    );
    assert_eq!(
        replicas.into_iter().next().expect("replica").into_parts(),
        ("orders".to_owned(), 3, 4096, -1, true)
    );
}
