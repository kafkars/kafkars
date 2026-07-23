//! Shard-turn post-stage deadline and scheduling scenarios.

use std::num::NonZeroUsize;

use kafka_client_core::{Deadline, Moment};

use crate::producer::pending::{PendingNotificationRouteMode, PendingNotificationRouteProgress};

use super::{
    pending_local_settlement::PendingLocalSettlementDisposition,
    shard_turn_progress::{blocked, min_deadline, runnable},
    shard_turn_test::{admit, fixture, input, register, run},
};

#[test]
fn deadline_merge_keeps_the_earliest_absolute_fact() {
    assert_eq!(
        min_deadline(Some(Deadline::from_tick(20)), Some(Deadline::from_tick(10))),
        Some(Deadline::from_tick(10))
    );
    assert_eq!(
        min_deadline(Some(Deadline::from_tick(20)), None),
        Some(Deadline::from_tick(20))
    );
}

#[test]
fn post_stage_deadline_selects_accepted_pending_and_promoted_facts() {
    let mut accepted_first = fixture();
    let accepted = admit(&mut accepted_first, "accepted", 10);
    let first = register(&mut accepted_first, "pending-first", 20);
    let second = register(&mut accepted_first, "pending-second", 30);
    let progress = run(&mut accepted_first, input(1, false, 1, 1));
    assert_eq!(progress.next_deadline(), Some(Deadline::from_tick(10)));
    drop((accepted, first, second));

    let mut pending_first = fixture();
    let accepted = admit(&mut pending_first, "accepted", 30);
    let first = register(&mut pending_first, "pending-first", 20);
    let second = register(&mut pending_first, "pending-second", 10);
    let progress = run(&mut pending_first, input(1, false, 1, 1));
    assert_eq!(progress.next_deadline(), Some(Deadline::from_tick(10)));
    drop((accepted, first, second));

    let mut promoted_first = fixture();
    let accepted = admit(&mut promoted_first, "accepted", 30);
    let pending = register(&mut promoted_first, "promoted", 12);
    let progress = run(&mut promoted_first, input(1, false, 1, 1));
    assert_eq!(progress.next_deadline(), Some(Deadline::from_tick(12)));
    drop((accepted, pending));
}

#[test]
fn promotion_created_materialization_is_immediately_runnable() {
    let mut data = fixture();
    let first = register(&mut data, "same-topic", 100);
    let second = register(&mut data, "same-topic", 100);
    let first_turn = run(&mut data, input(1, false, 1, 1));
    assert!(first_turn.runnable());

    let second_turn = run(&mut data, input(1, false, 1, 1));

    assert!(second_turn.promotion.is_some());
    assert_eq!(second_turn.snapshot.pending_records, 0);
    assert!(second_turn.runnable());
    assert!(!second_turn.blocked());
    drop((first, second));
}

#[test]
fn primary_route_backpressure_is_blocked_without_becoming_runnable() {
    let route = PendingNotificationRouteProgress::primary_for_test(1, true, true);

    assert!(blocked(None, None, route));
    assert!(!runnable(None, None, None, route, false, false));
}

#[test]
fn recovery_route_requires_terminal_handoff_instead_of_polling() {
    let mut data = fixture();
    let send = register(&mut data, "expired", 5);
    let local = data
        .settle_pending_local(Moment::from_tick(5), NonZeroUsize::MIN)
        .unwrap_or_else(|_failure| panic!("expiry should retain one route job"));
    assert_eq!(
        local.disposition(),
        PendingLocalSettlementDisposition::Expiry
    );
    let primary = data
        .host
        .completions
        .stop_notifier()
        .unwrap_or_else(|error| panic!("primary notifier should stop: {error}"));

    let progress = run(&mut data, input(6, false, 1, 1));

    assert_eq!(
        progress.route.mode(),
        PendingNotificationRouteMode::Recovery
    );
    assert!(progress.terminal_handoff());
    assert!(!progress.blocked());
    let shutdown = data.host.pending_notifications.begin_shutdown(primary);
    assert_eq!(
        shutdown.finish_notification_shutdown(),
        crate::producer::pending::PendingNotificationShutdownFailures::default()
    );
    drop(send);
}

#[test]
fn accepted_only_retained_ownership_prevents_shutdown_readiness() {
    let mut data = fixture();
    let accepted = admit(&mut data, "accepted", 100);

    let progress = run(&mut data, input(1, true, 1, 1));

    assert_eq!(progress.snapshot.pending_records, 0);
    assert_eq!(progress.snapshot.pending_bytes, 0);
    assert_eq!(progress.snapshot.pending_permits, 0);
    assert_eq!(progress.snapshot.route_retained, 0);
    assert_eq!(progress.snapshot.accepted_unsettled, 1);
    assert!(!progress.shutdown_ready());
    drop(accepted);
}

#[test]
fn empty_closed_snapshot_is_ready_for_authoritative_terminal_verification() {
    let mut data = fixture();

    let progress = run(&mut data, input(1, true, 1, 1));

    assert!(progress.shutdown_ready());
    assert_eq!(progress.snapshot.accepted_unsettled, 0);
    assert_eq!(progress.snapshot.pending_records, 0);
    assert_eq!(progress.snapshot.pending_bytes, 0);
    assert_eq!(progress.snapshot.pending_permits, 0);
    assert_eq!(progress.snapshot.route_retained, 0);
}
