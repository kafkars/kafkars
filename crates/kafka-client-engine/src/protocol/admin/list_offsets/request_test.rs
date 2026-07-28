//! One-partition Admin `ListOffsets` request construction scenarios.

use kafka_client_core::{AdminListOffsetSpec, AdminListOffsetTarget, ReadIsolation};

use super::{admin_list_offsets_request, request::AdminListOffsetsRequestFailure};

#[test]
fn every_kafka_43_spec_uses_its_exact_wire_value() {
    for (spec, expected) in [
        (AdminListOffsetSpec::Earliest, -2),
        (AdminListOffsetSpec::Latest, -1),
        (AdminListOffsetSpec::MaxTimestamp, -3),
        (AdminListOffsetSpec::EarliestLocal, -4),
        (AdminListOffsetSpec::LatestTiered, -5),
        (AdminListOffsetSpec::EarliestPendingUpload, -6),
        (
            AdminListOffsetSpec::Timestamp(1_700_000_000_123),
            1_700_000_000_123,
        ),
    ] {
        let request =
            admin_list_offsets_request(&target(spec), ReadIsolation::ReadUncommitted, 4_321)
                .unwrap_or_else(|error| panic!("valid request: {error:?}"));
        assert_eq!(request.replica_id, -1);
        assert_eq!(request.isolation_level, 0);
        assert_eq!(request.timeout_ms, 4_321);
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].name.as_str(), "audit-log");
        assert_eq!(request.topics[0].partitions.len(), 1);
        let partition = &request.topics[0].partitions[0];
        assert_eq!(partition.partition_index, 7);
        assert_eq!(partition.current_leader_epoch, -1);
        assert_eq!(partition.timestamp, expected);
    }
}

#[test]
fn read_committed_uses_exact_wire_isolation() {
    let request = admin_list_offsets_request(
        &target(AdminListOffsetSpec::Latest),
        ReadIsolation::ReadCommitted,
        4_321,
    )
    .unwrap_or_else(|error| panic!("valid read-committed request: {error:?}"));

    assert_eq!(request.isolation_level, 1);
}

#[test]
fn present_current_leader_epoch_uses_its_exact_wire_value() {
    let target = target(AdminListOffsetSpec::Latest).with_current_leader_epoch(Some(27));
    let request = admin_list_offsets_request(&target, ReadIsolation::ReadUncommitted, 4_321)
        .unwrap_or_else(|error| panic!("valid fenced request: {error:?}"));

    assert_eq!(request.topics[0].partitions[0].current_leader_epoch, 27);
}

#[test]
fn negative_remaining_timeout_never_reaches_generated_storage() {
    assert_eq!(
        admin_list_offsets_request(
            &target(AdminListOffsetSpec::Latest),
            ReadIsolation::ReadUncommitted,
            -1,
        ),
        Err(AdminListOffsetsRequestFailure::NegativeTimeout { actual: -1 })
    );
}

fn target(spec: AdminListOffsetSpec) -> AdminListOffsetTarget {
    AdminListOffsetTarget::new("audit-log".to_owned(), 7, spec)
}
