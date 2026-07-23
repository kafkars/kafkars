//! Exact local-settlement route and source-fault ownership scenarios.

use kafka_client_core::Deadline;

use crate::producer::{
    host_limits_test::{start, valid_limits},
    pending::{
        PendingLocalFailure, PendingNotificationJob, PendingNotificationRoute, ProducerSendFailure,
        ProducerSendFailureKind, turn_error::PendingTurnFailureOwnership,
    },
};

use super::{
    data::ProducerShardData,
    pending_fatal::PendingShardFatal,
    pending_local_fatal::{PendingLocalSettlementFatal, PendingLocalSettlementMode},
    pending_local_settlement::PendingLocalSettlementDisposition,
    pending_local_settlement_test::{assert_progress, register, settle},
};

#[test]
fn route_refusal_retains_current_job_and_untouched_expiry_suffix() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let route = PendingNotificationRoute::start(1)
        .unwrap_or_else(|error| panic!("bounded disagreement route should start: {error}"));
    drop(std::mem::replace(
        &mut data.host.pending_notifications,
        route,
    ));
    let first = register(&mut data, "first", 5).into_send();
    let second = register(&mut data, "second", 5).into_send();
    let third = register(&mut data, "third", 5).into_send();

    let progress = settle(&mut data, 5, 3);

    assert_progress(
        progress,
        PendingLocalSettlementDisposition::Faulted,
        (3, 1, true, false, None, 1),
    );
    let fatal = local_fatal(&data);
    assert_eq!(fatal.mode(), PendingLocalSettlementMode::Expiry);
    assert_eq!(fatal.inspected_for_test(), 3);
    assert_eq!(fatal.retained_prefix_for_test(), 1);
    assert_eq!(
        fatal
            .refused_failure_for_test()
            .map(ProducerSendFailure::kind),
        Some(ProducerSendFailureKind::DeadlineElapsed)
    );
    assert_eq!(
        fatal
            .refused_for_test()
            .and_then(PendingNotificationJob::permit_slot_for_test),
        Some(1)
    );
    assert!(fatal.source_for_test().is_none());
    let topics: Vec<_> = fatal
        .untouched_for_test()
        .iter()
        .map(PendingLocalFailure::topic_for_test)
        .collect();
    assert_eq!(topics, ["third"]);

    let snapshot = data.shard_stats();
    let faulted = settle(&mut data, 100, 3);
    assert_progress(
        faulted,
        PendingLocalSettlementDisposition::Faulted,
        (0, 0, true, false, None, 1),
    );
    assert_eq!(data.shard_stats(), snapshot);
    drop((first, second, third));
}

#[test]
fn failed_source_turn_keeps_ownership_after_routing_completed_prefix() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let first = register(&mut data, "first", 5);
    let second = register(&mut data, "second", 5);
    data.pending
        .remove_fifo_index_for_test(second.id())
        .unwrap_or_else(|error| panic!("test corruption should retain the entry: {error:?}"));

    let progress = settle(&mut data, 5, 2);

    assert_progress(
        progress,
        PendingLocalSettlementDisposition::Faulted,
        (2, 1, true, false, Some(Deadline::from_tick(5)), 1),
    );
    let fatal = local_fatal(&data);
    assert_eq!(fatal.retained_prefix_for_test(), 1);
    assert!(fatal.refused_for_test().is_none());
    assert!(fatal.untouched_for_test().is_empty());
    assert!(matches!(
        fatal.source_for_test(),
        Some(PendingTurnFailureOwnership::Take(_))
    ));
    drop((first.into_send(), second.into_send()));
}

#[test]
fn shutdown_route_refusal_is_the_only_closed_to_faulted_transition() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let first = register(&mut data, "first", 20).into_send();
    let second = register(&mut data, "second", 10).into_send();
    data.close_admission();
    let rollback = data.host.pending_notifications.begin_startup_rollback();

    let progress = settle(&mut data, 0, 2);

    assert_eq!(
        progress.disposition(),
        PendingLocalSettlementDisposition::Faulted
    );
    let fatal = local_fatal(&data);
    assert_eq!(fatal.mode(), PendingLocalSettlementMode::ShutdownDrain);
    assert_eq!(fatal.untouched_for_test().len(), 1);
    assert_eq!(
        fatal.untouched_for_test()[0].kind(),
        ProducerSendFailureKind::Shutdown
    );
    drop((first, second, rollback));
}

#[test]
fn closed_shard_returns_a_non_drain_local_fault_intact() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    data.close_admission();
    let incoming = PendingLocalSettlementFatal::source_failure(
        PendingLocalSettlementMode::Expiry,
        7,
        3,
        PendingTurnFailureOwnership::Registry,
    );

    let refused = data
        .retain_pending_local_fatal(incoming)
        .err()
        .unwrap_or_else(|| panic!("closed shard must refuse an expiry-generated fault"))
        .into_owner();

    assert_eq!(refused.mode(), PendingLocalSettlementMode::Expiry);
    assert_eq!(refused.inspected_for_test(), 7);
    assert_eq!(refused.retained_prefix_for_test(), 3);
    assert!(data.pending_fatal_for_test().is_none());
}

fn local_fatal(
    data: &ProducerShardData,
) -> &super::pending_local_fatal::PendingLocalSettlementFatal {
    let Some(PendingShardFatal::LocalSettlement(fatal)) = data.pending_fatal_for_test() else {
        panic!("local settlement must retain its exact first fault")
    };
    fatal
}
