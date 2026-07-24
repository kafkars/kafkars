//! Nonblocking `DescribeTopics` shard admission and close scenarios.

use std::sync::Arc;

use super::{
    DescribeTopicsAdmissionErrorKind, DescribeTopicsShardOwner, DescribeTopicsShardWake,
    DescribeTopicsShardWakeError,
};
use crate::admin::test_support::{describe_topics_host, stop_notifier};

struct NoopWake;

impl DescribeTopicsShardWake for NoopWake {
    fn wake(&self) -> Result<(), DescribeTopicsShardWakeError> {
        Ok(())
    }
}

#[test]
fn closed_port_rejects_without_reserving_terminal_capacity() {
    let (host, notifier) = describe_topics_host();
    let owner = DescribeTopicsShardOwner::new(host, Arc::new(NoopWake));
    let port = owner.admission_port();
    port.close_admission()
        .unwrap_or_else(|error| panic!("close admin admission: {error:?}"));
    let plan = kafka_client_core::DescribeTopicsPlan::new(vec!["orders".to_owned()])
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let deadline = crate::clock::OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(2),
        std::time::Instant::now() + std::time::Duration::from_secs(1),
    );
    let result = port.try_admit(
        kafka_client_core::Moment::from_tick(1),
        deadline,
        plan,
        16 * 1024,
    );
    assert!(matches!(
        result,
        Err(DescribeTopicsAdmissionErrorKind::Closed)
    ));
    drop(port);
    drop(owner);
    stop_notifier(notifier);
}
