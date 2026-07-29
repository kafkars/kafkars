//! Serial-route handoff and shutdown recovery scenarios for `IncrementalAlterConfigs`.

use std::time::Instant;

use kafka_client_core::{
    ConfigAlteration, IncrementalAlterConfigOutcome, IncrementalAlterConfigsBatch,
    IncrementalAlterConfigsInput, IncrementalAlterConfigsPlan, IncrementalAlterConfigsRoute,
    IncrementalConfigResourceAlteration,
};

use crate::clock::OperationDeadline;

use super::{
    IncrementalAlterConfigsDeliveryStatus, IncrementalAlterConfigsOutcome,
    IncrementalAlterConfigsTurn, model::IncrementalAlterConfigsRetention,
};

#[test]
fn later_route_recovery_preserves_prior_possible_delivery_at_every_handoff_boundary() {
    for phase in [
        LaterRoutePhase::Untouched,
        LaterRoutePhase::HandedOff,
        LaterRoutePhase::Submitted,
    ] {
        let plan = mixed_plan();
        let (mut host, notifier) = crate::admin::test_support::incremental_alter_configs_host();
        let admission = host
            .try_admit(
                kafka_client_core::Moment::from_tick(1),
                deadline(10),
                plan.clone(),
                IncrementalAlterConfigsRetention::from_parts(32 * 1024, result_limit_for(&plan)),
            )
            .unwrap_or_else(|error| panic!("admit mixed configs: {error:?}"));
        let IncrementalAlterConfigsTurn::Submit(first) = host
            .turn(kafka_client_core::Moment::from_tick(2))
            .unwrap_or_else(|error| panic!("take first route: {error}"))
        else {
            panic!("first route expected");
        };
        assert_eq!(first.route, IncrementalAlterConfigsRoute::AnyBroker);
        host.apply(
            first.operation_id,
            IncrementalAlterConfigsInput::DriverAccepted,
        )
        .unwrap_or_else(|error| panic!("accept first route: {error}"));
        host.apply(
            first.operation_id,
            IncrementalAlterConfigsInput::BrokerResponded {
                batch: IncrementalAlterConfigsBatch::new(
                    3,
                    vec![IncrementalAlterConfigOutcome::resource_altered(2, "orders")],
                ),
            },
        )
        .unwrap_or_else(|error| panic!("settle first route: {error}"));

        if phase != LaterRoutePhase::Untouched {
            let IncrementalAlterConfigsTurn::Submit(second) = host
                .turn(kafka_client_core::Moment::from_tick(3))
                .unwrap_or_else(|error| panic!("take second route: {error}"))
            else {
                panic!("second route expected");
            };
            assert_eq!(second.route, IncrementalAlterConfigsRoute::ExactBroker(1));
            if phase == LaterRoutePhase::Submitted {
                host.apply(
                    second.operation_id,
                    IncrementalAlterConfigsInput::DriverAccepted,
                )
                .unwrap_or_else(|error| panic!("accept second route: {error}"));
            }
        }

        host.recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("recover later route: {error}"));
        let IncrementalAlterConfigsOutcome::Failed(failure) = admission
            .observer
            .wait()
            .unwrap_or_else(|error| panic!("observe recovery: {error}"))
        else {
            panic!("recovery failure expected");
        };
        assert_eq!(
            failure.delivery(),
            IncrementalAlterConfigsDeliveryStatus::PossiblySent
        );
        drop(host);
        crate::admin::test_support::stop_notifier(notifier);
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum LaterRoutePhase {
    Untouched,
    HandedOff,
    Submitted,
}

fn mixed_plan() -> IncrementalAlterConfigsPlan {
    IncrementalAlterConfigsPlan::for_resources(
        vec![
            IncrementalConfigResourceAlteration::resource(
                2,
                "orders".to_owned(),
                vec![ConfigAlteration::delete("retention.ms".to_owned())],
            ),
            IncrementalConfigResourceAlteration::resource(
                4,
                "1".to_owned(),
                vec![ConfigAlteration::delete("log.cleaner.threads".to_owned())],
            ),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid mixed plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}

fn result_limit_for(plan: &IncrementalAlterConfigsPlan) -> usize {
    super::model::incremental_alter_configs_result_limit(plan)
        .unwrap_or_else(|| panic!("small result limit fits"))
}
