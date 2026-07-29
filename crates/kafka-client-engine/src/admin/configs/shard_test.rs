//! Nonblocking shard admission and close scenarios for `DescribeConfigs`.

use std::sync::Arc;

use super::{
    DescribeConfigsAdmissionErrorKind, DescribeConfigsRetention, DescribeConfigsShardOwner,
    DescribeConfigsShardWake, DescribeConfigsShardWakeError,
};

struct NoopWake;

impl DescribeConfigsShardWake for NoopWake {
    fn wake(&self) -> Result<(), DescribeConfigsShardWakeError> {
        Ok(())
    }
}

#[test]
fn closed_port_rejects_without_reserving_terminal_capacity() {
    let (host, notifier) = crate::admin::test_support::describe_configs_host();
    let owner = DescribeConfigsShardOwner::new(host, Arc::new(NoopWake));
    let port = owner.admission_port();
    port.close_admission()
        .unwrap_or_else(|error| panic!("close admin admission: {error:?}"));
    let plan = kafka_client_core::DescribeConfigsPlan::new(
        vec![kafka_client_core::DescribeConfigsResourceQuery::new(
            2,
            "orders".to_owned(),
            None,
        )],
        false,
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
            DescribeConfigsRetention::from_parts(16 * 1024, 8 * 1024),
        ),
        Err(DescribeConfigsAdmissionErrorKind::Closed)
    ));
    drop(port);
    drop(owner);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn generic_and_mixed_plans_reserve_one_bounded_operation() {
    let (host, notifier) = crate::admin::test_support::describe_configs_host();
    let owner = DescribeConfigsShardOwner::new(host, Arc::new(NoopWake));
    let port = owner.admission_port();
    let deadline = crate::clock::OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(10),
        std::time::Instant::now() + std::time::Duration::from_secs(1),
    );
    let accepted = port
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline,
            plan(&[
                (4, "7"),
                (2, "orders"),
                (32, "orders-workers"),
                (16, "telemetry"),
                (64, "future-resource"),
            ]),
            DescribeConfigsRetention::from_parts(16 * 1024, 8 * 1024),
        )
        .unwrap_or_else(|error| panic!("admit generic DescribeConfigs: {error:?}"));
    let host = owner
        .try_host()
        .unwrap_or_else(|error| panic!("inspect admitted host: {error:?}"));
    assert_eq!(host.unsettled(), 1);
    assert_eq!(host.retained_bytes_for_test(), 16 * 1024);
    drop(host);
    drop(accepted);
    drop(port);
    drop(owner);
    crate::admin::test_support::stop_notifier(notifier);
}

fn plan(resources: &[(i8, &str)]) -> kafka_client_core::DescribeConfigsPlan {
    kafka_client_core::DescribeConfigsPlan::new(
        resources
            .iter()
            .map(|(resource_type, resource_name)| {
                kafka_client_core::DescribeConfigsResourceQuery::new(
                    *resource_type,
                    (*resource_name).to_owned(),
                    None,
                )
            })
            .collect(),
        false,
        false,
    )
    .unwrap_or_else(|error| panic!("valid raw plan: {error}"))
}
