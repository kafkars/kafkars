//! Generated request grouping, validation, and retained-capacity scenarios.

use kafka_wire::RetainedSize;
use kafka_wire_core::{ApiVersion, BytesMut, KafkaEncode};

use super::{
    AlterReplicaLogDirAssignmentRef, AlterReplicaLogDirsRequestFailure,
    alter_replica_log_dirs_request,
};

#[test]
fn request_groups_paths_and_topics_deterministically_across_v1_v2() {
    let assignments = [
        assignment("zeta", 3, "/b"),
        assignment("alpha", 2, "/a"),
        assignment("alpha", 1, "/a"),
        assignment("zeta", 1, "/a"),
    ];
    let request = alter_replica_log_dirs_request(&assignments, usize::MAX)
        .unwrap_or_else(|error| panic!("valid request: {error:?}"));

    assert_eq!(request.dirs.len(), 2);
    assert_eq!(request.dirs[0].path.as_str(), "/a");
    assert_eq!(request.dirs[1].path.as_str(), "/b");
    assert_eq!(request.dirs[0].topics[0].name.as_str(), "alpha");
    assert_eq!(request.dirs[0].topics[0].partitions, [1, 2]);
    assert_eq!(request.dirs[0].topics[1].name.as_str(), "zeta");
    assert_eq!(request.dirs[0].topics[1].partitions, [1]);

    for version in 1..=2 {
        request
            .encode_into(&mut BytesMut::new(), ApiVersion::new(version))
            .unwrap_or_else(|error| panic!("v{version} request: {error:?}"));
    }
}

#[test]
fn invalid_and_duplicate_replica_assignments_are_rejected() {
    assert_eq!(
        alter_replica_log_dirs_request(&[], usize::MAX),
        Err(AlterReplicaLogDirsRequestFailure::EmptyAssignments)
    );
    assert_eq!(
        alter_replica_log_dirs_request(&[assignment("orders", -1, "/a")], usize::MAX),
        Err(AlterReplicaLogDirsRequestFailure::NegativePartition { actual: -1 })
    );
    let duplicate = [assignment("orders", 1, "/a"), assignment("orders", 1, "/b")];
    assert_eq!(
        alter_replica_log_dirs_request(&duplicate, usize::MAX),
        Err(AlterReplicaLogDirsRequestFailure::DuplicateReplica { partition: 1 })
    );
}

#[test]
fn complete_request_peak_must_fit_before_generated_ownership() {
    let assignments = [assignment("orders", 1, "/data")];
    let error = alter_replica_log_dirs_request(&assignments, 0).map_or_else(
        |error| error,
        |value| panic!("zero bytes cannot retain generated request: {value:?}"),
    );
    assert!(matches!(
        error,
        AlterReplicaLogDirsRequestFailure::RetainedBytes {
            required: 1..,
            limit: 0
        }
    ));

    let request = alter_replica_log_dirs_request(&assignments, usize::MAX)
        .unwrap_or_else(|error| panic!("bounded request: {error:?}"));
    assert!(request.retained_size().heap_bytes() > 0);
}

fn assignment<'a>(
    topic: &'a str,
    partition: i32,
    log_dir: &'a str,
) -> AlterReplicaLogDirAssignmentRef<'a> {
    AlterReplicaLogDirAssignmentRef::new(topic, partition, log_dir)
}
