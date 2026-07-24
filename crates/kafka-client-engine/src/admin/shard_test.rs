//! Nonblocking `CreateTopics` shard admission and close scenarios.

use std::sync::Arc;

use super::{
    CreateTopicsAdmissionErrorKind, CreateTopicsShardOwner, CreateTopicsShardWake,
    CreateTopicsShardWakeError, test_support::create_topics_host, test_support::stop_notifier,
};

struct NoopWake;

impl CreateTopicsShardWake for NoopWake {
    fn wake(&self) -> Result<(), CreateTopicsShardWakeError> {
        Ok(())
    }
}

#[test]
fn closed_port_rejects_without_reserving_terminal_capacity() {
    let (host, notifier) = create_topics_host();
    let owner = CreateTopicsShardOwner::new(host, Arc::new(NoopWake));
    let port = owner.admission_port();
    port.close_admission()
        .unwrap_or_else(|error| panic!("close admin admission: {error:?}"));
    let plan = kafka_client_core::CreateTopicsPlan::new(
        vec![kafka_client_core::CreateTopicSpecification::new(
            "orders",
            1,
            -1,
            Vec::new(),
        )],
        false,
    )
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
        Err(CreateTopicsAdmissionErrorKind::Closed)
    ));
    drop(port);
    drop(owner);
    stop_notifier(notifier);
}
