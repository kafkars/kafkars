//! Immediate shard admission, byte-bound, and close-state scenarios.

use std::time::Instant;

use kafka_client_core::{AdmissionRejection, Deadline, Moment};

use super::data::ProducerShardData;
use crate::{
    clock::OperationDeadline,
    producer::{
        ProducerRejectionReason,
        admission::ProducerAdmissionFailure,
        admission_test::record,
        host_limits_test::{start, valid_limits},
    },
};

#[test]
fn construction_installs_one_immediate_admission_owner() {
    let data = ProducerShardData::new(start(valid_limits()));
    let stats = data.shard_stats();

    assert_eq!(stats.host.store.records, 0);
    assert_eq!(stats.host.core_completion_slots, 0);
    assert!(stats.accepting);
}

#[test]
fn immediate_records_share_the_host_byte_ceiling() {
    let mut limits = valid_limits();
    limits.retained_bytes = 7;
    let mut data = ProducerShardData::new(start(limits));
    let accepted = data
        .try_admit_explicit(Moment::from_tick(1), deadline(), record("one"))
        .unwrap_or_else(|error| panic!("first record should be accepted: {error:?}"));

    let rejected = data.try_admit_explicit(Moment::from_tick(1), deadline(), record("two"));
    let Err(ProducerAdmissionFailure::Rejected(rejected)) = rejected else {
        panic!("host byte ceiling should reject the second record")
    };
    assert_eq!(
        rejected.reason(),
        ProducerRejectionReason::Store(crate::producer::ProducerStoreError::ByteCapacity)
    );
    assert_eq!(data.shard_stats().host.store.bytes, 4);
    drop(accepted);
}

#[test]
fn close_atomically_stops_immediate_and_core_admission() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    data.close_admission();

    let accepted = data.try_admit_explicit(Moment::from_tick(1), deadline(), record("core"));
    let Err(ProducerAdmissionFailure::Rejected(rejected)) = accepted else {
        panic!("core admission should reject after shard close")
    };
    assert_eq!(
        rejected.reason(),
        ProducerRejectionReason::Core(AdmissionRejection::Closed)
    );
    assert!(!data.shard_stats().accepting);
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(90), Instant::now())
}
