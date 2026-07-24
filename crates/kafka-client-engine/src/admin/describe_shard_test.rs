//! Nonblocking `DescribeCluster` shard close scenarios.

use std::sync::Arc;

use super::{
    DescribeClusterAdmissionErrorKind, DescribeClusterHost, DescribeClusterShardOwner,
    DescribeClusterShardWake, DescribeClusterShardWakeError,
};

struct NoopWake;

impl DescribeClusterShardWake for NoopWake {
    fn wake(&self) -> Result<(), DescribeClusterShardWakeError> {
        Ok(())
    }
}

#[test]
fn closed_port_rejects_before_terminal_reservation() {
    let host = DescribeClusterHost::new()
        .unwrap_or_else(|error| panic!("start DescribeCluster host: {error}"));
    let owner = DescribeClusterShardOwner::new(host, Arc::new(NoopWake));
    let port = owner.admission_port();
    port.close_admission()
        .unwrap_or_else(|error| panic!("close admission: {error:?}"));
    let deadline = crate::clock::OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(2),
        std::time::Instant::now() + std::time::Duration::from_secs(1),
    );
    assert!(matches!(
        port.try_admit(kafka_client_core::Moment::from_tick(1), deadline),
        Err(DescribeClusterAdmissionErrorKind::Closed)
    ));
    let notifier = owner
        .terminal_host()
        .stop_notifier()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    notifier
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("join notifier: {error}"));
}
