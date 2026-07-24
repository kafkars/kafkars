//! Nonblocking incremental configuration shard admission and close scenarios.

use std::sync::Arc;

use super::{
    IncrementalAlterConfigsAdmissionErrorKind,
    model::IncrementalAlterConfigsRetention,
    shard::{
        IncrementalAlterConfigsShardOwner, IncrementalAlterConfigsShardWake,
        IncrementalAlterConfigsShardWakeError,
    },
};

struct NoopWake;

impl IncrementalAlterConfigsShardWake for NoopWake {
    fn wake(&self) -> Result<(), IncrementalAlterConfigsShardWakeError> {
        Ok(())
    }
}

#[test]
fn closed_port_rejects_before_terminal_or_retained_capacity_is_reserved() {
    let (host, notifier) = crate::admin::test_support::incremental_alter_configs_host();
    let owner = IncrementalAlterConfigsShardOwner::new(host, Arc::new(NoopWake));
    let port = owner.admission_port();
    {
        let host = owner
            .try_host()
            .unwrap_or_else(|error| panic!("inspect incremental config host: {error:?}"));
        assert_eq!(host.unsettled(), 0);
    }
    port.close_admission()
        .unwrap_or_else(|error| panic!("close incremental config admission: {error:?}"));
    let deadline = crate::clock::OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(2),
        std::time::Instant::now() + std::time::Duration::from_secs(1),
    );
    assert!(matches!(
        port.try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline,
            plan(),
            IncrementalAlterConfigsRetention::from_parts(16 * 1024, 8 * 1024),
        ),
        Err(IncrementalAlterConfigsAdmissionErrorKind::Closed)
    ));

    drop(port);
    {
        let host = owner.terminal_host();
        assert_eq!(host.unsettled(), 0);
    }
    drop(owner);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn wake_error_retains_its_io_cause_without_becoming_operation_policy() {
    let error = IncrementalAlterConfigsShardWakeError::from_io(std::io::Error::other("closed"));
    assert!(error.to_string().contains("closed"));
}

fn plan() -> kafka_client_core::IncrementalAlterConfigsPlan {
    kafka_client_core::IncrementalAlterConfigsPlan::new(
        vec![kafka_client_core::TopicConfigAlteration::new(
            "orders".to_owned(),
            vec![kafka_client_core::ConfigAlteration::delete(
                "retention.ms".to_owned(),
            )],
        )],
        false,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}
