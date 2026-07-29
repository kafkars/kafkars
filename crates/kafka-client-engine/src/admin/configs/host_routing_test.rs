//! Serial route fan-out scenarios for one bounded `DescribeConfigs` operation.

use std::time::{Duration, Instant};

use kafka_client_core::{
    DescribeConfigOutcome, DescribeConfigsBatch as CoreDescribeConfigsBatch, DescribeConfigsInput,
    DescribeConfigsPlan, DescribeConfigsResourceQuery, DescribeConfigsRoute, OperationId,
};

use crate::clock::OperationDeadline;

use super::{
    DescribeConfigsBatch, DescribeConfigsDeliveryStatus, DescribeConfigsFailureKind,
    DescribeConfigsHost, DescribeConfigsObserver, DescribeConfigsOutcome, DescribeConfigsRetention,
    DescribeConfigsTurn,
};

#[test]
fn mixed_routes_reuse_one_operation_deadline_and_partition_result_capacity() {
    let (mut host, notifier) = crate::admin::test_support::describe_configs_host();
    let original_deadline = deadline(100);
    let admission = admit(&mut host, original_deadline);

    let operation_id = settle_route(
        &mut host,
        2,
        None,
        original_deadline,
        DescribeConfigsRoute::ExactBroker(7),
        &[(4, "7"), (8, "7")],
        11,
    );
    settle_route(
        &mut host,
        3,
        Some(operation_id),
        original_deadline,
        DescribeConfigsRoute::AnyBroker,
        &[(2, "orders")],
        37,
    );
    settle_route(
        &mut host,
        4,
        Some(operation_id),
        original_deadline,
        DescribeConfigsRoute::ExactBroker(3),
        &[(4, "3")],
        19,
    );

    let DescribeConfigsOutcome::Configs(batch) = admission
        .wait()
        .unwrap_or_else(|error| panic!("observe mixed terminal: {error}"))
    else {
        panic!("mixed routes must produce one successful terminal");
    };
    assert_eq!(batch.throttle_time_ms(), 37);
    assert_eq!(
        identities(&batch),
        [(4, "7"), (2, "orders"), (8, "7"), (4, "3")]
    );
    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn later_local_rejection_keeps_prior_route_delivery_visible() {
    let (mut host, notifier) = crate::admin::test_support::describe_configs_host();
    let original_deadline = deadline(100);
    let admission = admit(&mut host, original_deadline);
    settle_route(
        &mut host,
        2,
        None,
        original_deadline,
        DescribeConfigsRoute::ExactBroker(7),
        &[(4, "7"), (8, "7")],
        11,
    );
    let DescribeConfigsTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("take second route: {error}"))
    else {
        panic!("second route must be ready");
    };
    host.apply(
        submission.operation_id,
        DescribeConfigsInput::DriverRejected,
    )
    .unwrap_or_else(|error| panic!("reject second route: {error}"));
    let DescribeConfigsOutcome::Failed(failure) = admission
        .wait()
        .unwrap_or_else(|error| panic!("observe route failure: {error}"))
    else {
        panic!("route rejection must fail the whole operation");
    };
    assert_eq!(failure.kind(), DescribeConfigsFailureKind::DriverRejected);
    assert_eq!(
        failure.delivery(),
        DescribeConfigsDeliveryStatus::PossiblySent
    );
    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

fn admit(host: &mut DescribeConfigsHost, deadline: OperationDeadline) -> DescribeConfigsObserver {
    host.try_admit(
        kafka_client_core::Moment::from_tick(1),
        deadline,
        mixed_plan(),
        DescribeConfigsRetention::from_parts(2 * 1024 * 1024, 4 * 256 * 1024),
    )
    .unwrap_or_else(|error| panic!("admit mixed DescribeConfigs: {error:?}"))
    .observer
}

fn settle_route(
    host: &mut DescribeConfigsHost,
    now: u64,
    expected_operation_id: Option<OperationId>,
    expected_deadline: OperationDeadline,
    expected_route: DescribeConfigsRoute,
    resources: &[(i8, &str)],
    throttle_time_ms: u32,
) -> OperationId {
    let DescribeConfigsTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(now))
        .unwrap_or_else(|error| panic!("take route submission: {error}"))
    else {
        panic!("route submission must be ready");
    };
    let (operation_id, deadline, route, plan, result_limit) = submission.into_parts();
    assert_eq!(expected_operation_id.unwrap_or(operation_id), operation_id);
    assert_eq!(deadline, expected_deadline);
    assert_eq!(route, expected_route);
    assert_eq!(result_limit, resources.len() * 256 * 1024);
    assert_eq!(plan_identities(&plan), resources);
    host.apply(operation_id, DescribeConfigsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept route: {error}"));
    host.apply(
        operation_id,
        DescribeConfigsInput::BrokerResponded {
            batch: CoreDescribeConfigsBatch::new(
                throttle_time_ms,
                resources
                    .iter()
                    .map(|(kind, name)| DescribeConfigOutcome::described(*kind, *name, Vec::new()))
                    .collect(),
            ),
        },
    )
    .unwrap_or_else(|error| panic!("settle route: {error}"));
    operation_id
}

fn mixed_plan() -> DescribeConfigsPlan {
    DescribeConfigsPlan::new(
        vec![
            DescribeConfigsResourceQuery::new(4, "7".to_owned(), None),
            DescribeConfigsResourceQuery::new(2, "orders".to_owned(), None),
            DescribeConfigsResourceQuery::new(8, "7".to_owned(), None),
            DescribeConfigsResourceQuery::new(4, "3".to_owned(), None),
        ],
        false,
        false,
    )
    .unwrap_or_else(|error| panic!("valid mixed plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + Duration::from_secs(1),
    )
}

fn plan_identities(plan: &DescribeConfigsPlan) -> Vec<(i8, &str)> {
    plan.resources()
        .iter()
        .map(|resource| (resource.resource_type(), resource.resource_name()))
        .collect()
}

fn identities(batch: &DescribeConfigsBatch) -> Vec<(i8, &str)> {
    batch
        .resources
        .iter()
        .map(|resource| (resource.resource_type, resource.resource_name.as_str()))
        .collect()
}
