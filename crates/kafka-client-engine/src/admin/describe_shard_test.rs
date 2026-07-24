//! Nonblocking `DescribeCluster` shard close scenarios.

use std::sync::Arc;

use super::{
    DescribeClusterAdmissionErrorKind, DescribeClusterShardOwner, DescribeClusterShardWake,
    DescribeClusterShardWakeError, test_support::describe_cluster_host,
    test_support::stop_notifier,
};

struct NoopWake;

impl DescribeClusterShardWake for NoopWake {
    fn wake(&self) -> Result<(), DescribeClusterShardWakeError> {
        Ok(())
    }
}

#[test]
fn closed_port_rejects_before_terminal_reservation() {
    let (host, notifier) = describe_cluster_host();
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
    drop(port);
    drop(owner);
    stop_notifier(notifier);
}
