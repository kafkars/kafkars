//! Caller-order and exact per-replica error preservation tests.

use std::time::Duration;

use crate::{ErrorKind, KafkaError, admin::BatchResult};

use super::{AlterReplicaLogDirsResult, TopicPartitionReplica};

#[test]
fn result_preserves_caller_order_and_exact_replica_identity() {
    let first = TopicPartitionReplica::new("orders", 2, 7);
    let second = TopicPartitionReplica::new("audit", 0, 3);
    let result = AlterReplicaLogDirsResult::new(
        Duration::from_millis(11),
        BatchResult::new(vec![
            (first.clone(), Ok(())),
            (
                second.clone(),
                Err(KafkaError::new(
                    ErrorKind::Broker,
                    "Kafka rejected replica log-directory alteration",
                )
                .with_broker_code(Some(57))),
            ),
        ]),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(11));
    assert_eq!(result.replicas().entries()[0].0, first);
    assert_eq!(result.replicas().entries()[1].0, second);
    assert_eq!(
        result.replicas().entries()[1]
            .1
            .as_ref()
            .expect_err("second replica must preserve its broker error")
            .broker_code(),
        Some(57)
    );
    assert_eq!(result.into_replicas().into_entries().len(), 2);
}
