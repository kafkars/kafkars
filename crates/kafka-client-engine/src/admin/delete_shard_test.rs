//! Nonblocking `DeleteTopics` shard admission and close scenarios.

use std::sync::Arc;

use super::{
    DeleteTopicsAdmissionErrorKind, DeleteTopicsHost, DeleteTopicsShardOwner,
    DeleteTopicsShardWake, DeleteTopicsShardWakeError,
};

struct NoopWake;

impl DeleteTopicsShardWake for NoopWake {
    fn wake(&self) -> Result<(), DeleteTopicsShardWakeError> {
        Ok(())
    }
}

#[test]
fn closed_port_rejects_without_reserving_terminal_capacity() {
    let host =
        DeleteTopicsHost::new().unwrap_or_else(|error| panic!("start deletion host: {error}"));
    let owner = DeleteTopicsShardOwner::new(host, Arc::new(NoopWake));
    let port = owner.admission_port();
    port.close_admission()
        .unwrap_or_else(|error| panic!("close deletion admission: {error:?}"));
    let plan = kafka_client_core::DeleteTopicsPlan::new(vec!["orders".to_owned()])
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
        Err(DeleteTopicsAdmissionErrorKind::Closed)
    ));
    let notifier = owner
        .terminal_host()
        .stop_notifier()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    notifier
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("join notifier: {error}"));
}
