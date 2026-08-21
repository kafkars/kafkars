//! Selected-replica operation ready-result observation coverage.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Wake, Waker},
};

use crate::{
    ErrorKind, KafkaError, bridge::admin_describe_replica_log_dirs::AdminDescribeReplicaLogDirs,
};

use super::DescribeReplicaLogDirs;

#[test]
fn named_operation_forwards_one_ready_error() {
    let mut operation =
        DescribeReplicaLogDirs::from_bridge(AdminDescribeReplicaLogDirs::ready_for_test(Err(
            KafkaError::new(ErrorKind::Configuration, "invalid request"),
        )));
    let waker = Waker::from(std::sync::Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);

    assert!(matches!(
        Pin::new(&mut operation).poll(&mut context),
        Poll::Ready(Err(error)) if error.kind() == ErrorKind::Configuration
    ));
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: std::sync::Arc<Self>) {}
}
