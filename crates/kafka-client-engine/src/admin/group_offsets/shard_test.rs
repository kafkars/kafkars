//! Nonblocking group-offset shard admission and close scenarios.

use std::sync::Arc;

use kafka_client_core::{ListConsumerGroupOffsetsPlan, Moment};

use super::{
    ListConsumerGroupOffsetsAdmissionErrorKind, ListConsumerGroupOffsetsShardOwner,
    ListConsumerGroupOffsetsShardWake, ListConsumerGroupOffsetsShardWakeError,
};

struct NoopWake;

impl ListConsumerGroupOffsetsShardWake for NoopWake {
    fn wake(&self) -> Result<(), ListConsumerGroupOffsetsShardWakeError> {
        Ok(())
    }
}

#[test]
fn closed_port_rejects_before_completion_or_bytes_are_reserved() {
    let (host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let owner = ListConsumerGroupOffsetsShardOwner::new(host, Arc::new(NoopWake));
    let port = owner.admission_port();
    port.close_admission()
        .unwrap_or_else(|error| panic!("close group-offset admission: {error:?}"));

    assert!(matches!(
        port.try_admit(Moment::from_tick(1), deadline(), plan()),
        Err(ListConsumerGroupOffsetsAdmissionErrorKind::Closed)
    ));
    assert_eq!(owner.terminal_host().unsettled(), 0);

    drop((port, owner));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn wake_error_retains_its_io_cause_without_becoming_policy() {
    let error = ListConsumerGroupOffsetsShardWakeError::from_io(std::io::Error::other("closed"));
    assert!(error.to_string().contains("closed"));
}

fn plan() -> ListConsumerGroupOffsetsPlan {
    ListConsumerGroupOffsetsPlan::new("payments".to_owned(), false)
        .unwrap_or_else(|error| panic!("valid group-offset plan: {error}"))
}

fn deadline() -> crate::clock::OperationDeadline {
    crate::clock::OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(2),
        std::time::Instant::now() + std::time::Duration::from_secs(1),
    )
}
