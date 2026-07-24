//! Nonblocking `CreatePartitions` shard admission and close scenarios.

use std::sync::Arc;

use super::{
    CreatePartitionsAdmissionErrorKind, CreatePartitionsShardOwner, CreatePartitionsShardWake,
    CreatePartitionsShardWakeError,
};
use crate::admin::test_support::{create_partitions_host, stop_notifier};

struct NoopWake;

impl CreatePartitionsShardWake for NoopWake {
    fn wake(&self) -> Result<(), CreatePartitionsShardWakeError> {
        Ok(())
    }
}

#[test]
fn closed_port_rejects_without_reserving_terminal_capacity() {
    let (host, notifier) = create_partitions_host();
    let owner = CreatePartitionsShardOwner::new(host, Arc::new(NoopWake));
    let port = owner.admission_port();
    port.close_admission()
        .unwrap_or_else(|error| panic!("close partition admission: {error:?}"));
    let plan = kafka_client_core::CreatePartitionsPlan::new(
        vec![kafka_client_core::CreatePartitionsSpecification::new(
            "orders".to_owned(),
            8,
        )],
        false,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let deadline = crate::clock::OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(2),
        std::time::Instant::now() + std::time::Duration::from_secs(1),
    );
    assert!(matches!(
        port.try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline,
            plan,
            16 * 1024,
        ),
        Err(CreatePartitionsAdmissionErrorKind::Closed)
    ));
    drop((port, owner));
    stop_notifier(notifier);
}
