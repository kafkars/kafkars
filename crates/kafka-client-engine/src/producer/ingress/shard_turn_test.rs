//! Composite producer-shard stage ordering and lifecycle scenarios.

use std::{num::NonZeroUsize, time::Instant};

use kafka_client_core::{ByteCount, Deadline, Moment, ProducerBatchPolicy};

use crate::{
    clock::OperationDeadline,
    producer::{
        ProducerHostInvariantError,
        admission::AdmittedExplicit,
        admission_test::record,
        host_limits_test::{start, valid_limits},
        host_turn::ProducerTurnBudget,
    },
};

use super::{
    data::ProducerShardData,
    pending_fatal::PendingShardFatal,
    pending_local_settlement::PendingLocalSettlementDisposition,
    pending_settlement::PendingSettlementDisposition,
    promotion_error::PendingPromotionInvariant,
    shard_turn::ProducerShardTurnInput,
    shard_turn_failure::ProducerShardTurnFailureCause,
    shard_turn_progress::{ProducerShardTurnProgress, ProducerShardTurnState},
};

#[test]
fn due_expiry_tail_suppresses_promotion_but_other_stages_run() {
    let mut data = fixture();
    let first_pending = register(&mut data, "first-pending", 5);
    let second_pending = register(&mut data, "second-pending", 5);
    let first_accepted = admit(&mut data, "accepted", 100);
    let second_accepted = admit(&mut data, "accepted", 100);

    let progress = run(&mut data, input(5, false, 1, 1));

    assert_eq!(
        progress
            .local
            .unwrap_or_else(|| panic!("local stage should complete"))
            .disposition(),
        PendingLocalSettlementDisposition::Expiry
    );
    assert!(
        progress
            .local
            .is_some_and(super::pending_local_settlement::PendingLocalSettlementProgress::runnable)
    );
    assert_eq!(
        progress
            .accepted
            .unwrap_or_else(|| panic!("accepted stage should complete"))
            .prepared_effects,
        1
    );
    assert_eq!(progress.promotion, None);
    assert_eq!(progress.route.attempted(), 1);
    assert_eq!(progress.snapshot.pending_records, 1);
    drop((
        first_pending,
        second_pending,
        first_accepted,
        second_accepted,
    ));
}

#[test]
fn clear_expiry_stage_allows_exactly_one_fifo_promotion_and_retry() {
    let mut data = fixture();
    let first = register(&mut data, "first", 100);
    let second = register(&mut data, "second", 100);

    let progress = run(&mut data, input(1, false, 2, 1));

    let promotion = progress
        .promotion
        .unwrap_or_else(|| panic!("one pending head should promote"));
    assert_eq!(promotion.inspected(), 1);
    assert_eq!(
        promotion.disposition(),
        PendingSettlementDisposition::Productive
    );
    assert!(promotion.remaining());
    assert_eq!(progress.route.attempted(), 1);
    assert_eq!(progress.snapshot.pending_records, 1);
    assert!(progress.runnable());
    drop((first, second));
}

#[test]
fn restored_blocked_promotion_is_preserved_by_the_composite_turn() {
    let mut limits = valid_limits();
    limits.completion_capacity = 1;
    limits.record_capacity = 1;
    limits.batch_capacity = 1;
    limits.timer_capacity = 1;
    limits.pending_record_capacity = 1;
    limits.pending_notification_capacity = 1;
    limits.notification_capacity = 2;
    limits.batch_policy = ProducerBatchPolicy::try_new(1, ByteCount::new(64), 100)
        .unwrap_or_else(|error| panic!("single-record policy should validate: {error}"));
    let mut data = ProducerShardData::new(start(limits));
    let pending = register(&mut data, "pending", 100);
    let accepted = admit(&mut data, "accepted", 100);

    let progress = run(&mut data, input(2, false, 1, 1));

    let promotion = progress
        .promotion
        .unwrap_or_else(|| panic!("eligible head should attempt promotion"));
    assert_eq!(
        promotion.disposition(),
        PendingSettlementDisposition::RestoredBlocked
    );
    assert!(promotion.remaining());
    assert!(progress.blocked());
    assert_eq!(progress.snapshot.pending_records, 1);
    drop((pending, accepted));
}

#[test]
fn close_drains_pending_before_accepted_work_and_never_promotes() {
    let mut data = fixture();
    let first = register(&mut data, "first", 100);
    let second = register(&mut data, "second", 100);
    let accepted = admit(&mut data, "accepted", 100);

    let progress = run(&mut data, input(1, true, 1, 1));

    assert_eq!(progress.state(), ProducerShardTurnState::Closed);
    assert_eq!(
        progress
            .local
            .unwrap_or_else(|| panic!("close drain should complete"))
            .disposition(),
        PendingLocalSettlementDisposition::ShutdownDrain
    );
    assert_eq!(progress.promotion, None);
    assert_eq!(progress.route.attempted(), 1);
    assert_eq!(progress.snapshot.pending_records, 1);
    assert!(!progress.shutdown_ready());
    drop((first, second, accepted));
}

#[test]
fn reachable_promotion_fault_still_retries_its_notification_route() {
    let mut data = fixture();
    let send = register(&mut data, "promotion-fault", 100);
    let expected = ProducerHostInvariantError::MissingAdmissionIdentity;
    data.inject_post_acceptance_fault(expected);

    let progress = run(&mut data, input(1, false, 1, 1));

    assert_eq!(
        progress
            .promotion
            .unwrap_or_else(|| panic!("pending head should reach promotion"))
            .disposition(),
        PendingSettlementDisposition::Faulted
    );
    assert_eq!(progress.route.attempted(), 1);
    assert_eq!(progress.snapshot.route_retained, 0);
    assert_eq!(progress.state(), ProducerShardTurnState::Faulted);
    assert!(progress.terminal_handoff());
    let Some(PendingShardFatal::AcceptedInvariant(facts)) = data.pending_fatal_for_test() else {
        panic!("reachable promotion fault should retain the exact first owner")
    };
    assert_eq!(facts.invariant, PendingPromotionInvariant::Host(expected));
    drop(send);
}

#[test]
fn existing_pending_fault_skips_local_mutation_but_runs_accepted_and_route() {
    let mut data = fixture();
    let expired = register(&mut data, "expired", 5);
    let retained = register(&mut data, "retained", 100);
    let local = data
        .settle_pending_local(Moment::from_tick(5), nonzero(1))
        .unwrap_or_else(|_failure| panic!("expiry should retain its route job"));
    assert_eq!(local.notifications_retained(), 1);
    data.retain_pending_fatal(PendingShardFatal::accepted_invariant(
        None,
        PendingPromotionInvariant::Host(ProducerHostInvariantError::MissingAdmissionIdentity),
    ))
    .unwrap_or_else(|_failure| panic!("first pending fault should install"));
    let before = data.shard_stats().pending;

    let progress = run(&mut data, input(6, false, 2, 1));

    assert_eq!(progress.state(), ProducerShardTurnState::Faulted);
    assert!(progress.terminal_handoff());
    assert_eq!(
        progress
            .local
            .unwrap_or_else(|| panic!("faulted local stage should report"))
            .disposition(),
        PendingLocalSettlementDisposition::Faulted
    );
    assert!(progress.accepted.is_some());
    assert_eq!(progress.route.attempted(), 1);
    let after = data.shard_stats().pending;
    assert_eq!(after.records, before.records);
    assert_eq!(after.retained_bytes, before.retained_bytes);
    assert_eq!(after.accepting, before.accepting);
    drop((expired, retained));
}

#[test]
fn accepted_poison_closes_admission_retries_route_and_never_promotes() {
    let mut data = fixture();
    let expired = register(&mut data, "expired", 5);
    let retained = register(&mut data, "retained", 100);
    let local = data
        .settle_pending_local(Moment::from_tick(5), nonzero(1))
        .unwrap_or_else(|_failure| panic!("expiry should retain its route job"));
    assert_eq!(local.notifications_retained(), 1);
    assert_eq!(local.route_pending(), 1);
    let expected = ProducerHostInvariantError::PendingEffectCapacity;
    assert_eq!(data.host.poison(expected), expected);

    let failure = match data.shard_turn(input(6, false, 2, 1)) {
        Err(failure) => failure,
        Ok(_progress) => panic!("poisoned accepted host requires terminal handoff"),
    };
    let progress = failure.progress();

    assert_eq!(failure.accepted_invariant(), Some(expected));
    assert_eq!(progress.state(), ProducerShardTurnState::Closed);
    assert!(progress.terminal_handoff());
    assert_eq!(progress.promotion, None);
    assert_eq!(progress.route.attempted(), 1);
    assert!(!data.shard_stats().accepting);
    assert!(!data.shard_stats().pending.accepting);
    let (_progress, cause) = failure.into_parts();
    assert!(matches!(cause, ProducerShardTurnFailureCause::Host(error) if error == expected));
    drop((expired, retained));
}

pub(super) fn fixture() -> ProducerShardData {
    ProducerShardData::new(start(valid_limits()))
}

pub(super) fn run(
    data: &mut ProducerShardData,
    input: ProducerShardTurnInput,
) -> ProducerShardTurnProgress {
    data.shard_turn(input)
        .unwrap_or_else(|_failure| panic!("fixture shard turn should succeed"))
}

pub(super) fn input(
    now: u64,
    close_requested: bool,
    pending_local: usize,
    pending_route: usize,
) -> ProducerShardTurnInput {
    ProducerShardTurnInput {
        now: Moment::from_tick(now),
        close_requested,
        accepted: ProducerTurnBudget::try_new(1, 1, 1, 1, 1)
            .unwrap_or_else(|| panic!("one is a valid accepted-stage budget")),
        pending_local: nonzero(pending_local),
        pending_route: nonzero(pending_route),
    }
}

pub(super) fn register(
    data: &mut ProducerShardData,
    topic: &str,
    deadline: u64,
) -> crate::ProducerSend {
    data.register_pending(record(topic), operation_deadline(deadline))
        .unwrap_or_else(|error| panic!("pending fixture should register: {error:?}"))
        .into_send()
}

pub(super) fn admit(data: &mut ProducerShardData, topic: &str, deadline: u64) -> AdmittedExplicit {
    data.try_admit_explicit(
        Moment::from_tick(0),
        operation_deadline(deadline),
        record(topic),
    )
    .unwrap_or_else(|error| panic!("accepted fixture should admit: {error:?}"))
}

fn operation_deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), Instant::now())
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("turn budget must be nonzero"))
}
