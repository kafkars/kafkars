//! Admission, exact-broker sequencing, recovery, and byte ownership scenarios.

use std::sync::Arc;

use kafka_client_core::{DescribeReplicaLogDirsPlan, DescribeReplicaLogDirsReplica, Moment};

use crate::{
    admin::{AdminCompletionNotifier, DescribeReplicaLogDirsHost, DescribeReplicaLogDirsTurn},
    clock::MonotonicClock,
};

#[test]
fn admission_reserves_before_machine_creation_and_preserves_broker_order() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeReplicaLogDirsHost::new(ports.describe_replica_log_dirs);
    let clock = Arc::new(MonotonicClock::new());
    let capture = clock
        .capture_deadline_after(std::time::Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline: {error}"));
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            DescribeReplicaLogDirsPlan::new(vec![
                replica("orders", 0, 9),
                replica("audit", 1, 2),
                replica("orders", 2, 9),
            ])
            .unwrap_or_else(|error| panic!("plan: {error}")),
        )
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    assert!(admission.fault.is_none());
    assert!(host.retained_bytes_for_test() > 0);
    let DescribeReplicaLogDirsTurn::Submit(submission) = host
        .turn(Moment::from_tick(capture.now().tick()))
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("expected first submission");
    };
    let (_, _, broker_id, replicas, retained_limit) = submission.into_parts();
    assert_eq!(broker_id, 9);
    assert_eq!(replicas.len(), 2);
    assert!(retained_limit > 0);

    drop(admission.observer);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover: {error}"));
    assert_eq!(host.unsettled(), 0);
    drop(host);
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}

fn replica(topic: &str, partition: i32, broker_id: i32) -> DescribeReplicaLogDirsReplica {
    DescribeReplicaLogDirsReplica::new(topic.to_owned(), partition, broker_id)
}
