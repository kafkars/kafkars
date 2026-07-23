//! Dormant first-fault retention and terminal-refusal scenarios.

use std::time::Instant;

use kafka_client_core::{AdmissionRejection, Deadline, Moment};

use crate::{
    ProducerSend,
    clock::OperationDeadline,
    producer::{
        ProducerHostInvariantError, ProducerRejectionReason,
        admission::ProducerAdmissionFailure,
        admission_test::record,
        host_limits_test::{start, valid_limits},
        pending::{
            PendingAdmissionRejectionReason, PendingAttemptStateError, ProducerSendFailureKind,
        },
    },
};

use super::{
    data::ProducerShardData,
    pending_fatal::{PendingNotificationContext, PendingShardFatal},
    pending_settlement::PendingSettlementDisposition,
    promotion_error::PendingPromotionFailure,
    terminal::ProducerShardTerminalError,
};

#[test]
fn first_fault_closes_both_admission_paths_and_blocks_terminal_cleanup() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let (fatal, send, expected_deadline) = fatal_with_deadline(11);

    data.retain_pending_fatal(fatal)
        .unwrap_or_else(|_refusal| panic!("running shard must retain its first fatal owner"));

    let pending = match data.register_pending(record("pending-after-fault"), deadline(30)) {
        Err(pending) => pending,
        Ok(_registration) => panic!("fatal shard must reject pending admission"),
    };
    assert_eq!(pending.reason(), PendingAdmissionRejectionReason::Closed);
    assert_eq!(
        pending.into_record().topic().as_ref(),
        "pending-after-fault"
    );

    let immediate = data.try_admit_explicit(
        Moment::from_tick(1),
        deadline(30),
        record("core-after-fault"),
    );
    let Err(ProducerAdmissionFailure::Rejected(immediate)) = immediate else {
        panic!("fatal shard must reject immediate admission")
    };
    assert_eq!(
        immediate.reason(),
        ProducerRejectionReason::Core(AdmissionRejection::Closed)
    );
    assert_eq!(immediate.into_record().topic().as_ref(), "core-after-fault");
    assert!(!data.shard_stats().accepting);
    assert!(!data.shard_stats().pending.accepting);
    assert!(matches!(
        data.verify_terminal_cleanup(),
        Err(ProducerShardTerminalError::PendingFatal)
    ));
    assert_eq!(fatal_deadline(&data), expected_deadline);
    drop(send);
}

#[test]
fn later_fault_and_explicit_close_preserve_the_first_owner() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let (first, first_send, first_deadline) = fatal_with_deadline(11);
    let (later, later_send, later_deadline) = fatal_with_deadline(22);
    data.retain_pending_fatal(first)
        .unwrap_or_else(|_refusal| panic!("first fatal owner must win"));
    assert_eq!(fatal_deadline(&data), first_deadline);

    let refused = match data.retain_pending_fatal(later) {
        Err(refused) => refused.into_owner(),
        Ok(()) => panic!("a later fatal owner must be returned intact"),
    };
    assert_eq!(owner_deadline(&refused), later_deadline);
    assert_eq!(fatal_deadline(&data), first_deadline);

    data.close_admission();
    assert_eq!(fatal_deadline(&data), first_deadline);
    assert!(matches!(
        data.verify_release_before_completion(),
        Err(ProducerShardTerminalError::PendingFatal)
    ));
    drop((first_send, later_send, refused));
}

#[test]
fn generic_fatal_cannot_convert_an_explicitly_closed_shard() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    data.close_admission();
    let (incoming, send, expected_deadline) = fatal_with_deadline(33);

    let refused = match data.retain_pending_fatal(incoming) {
        Err(refused) => refused.into_owner(),
        Ok(()) => panic!("generic fatal retention must not bypass explicit closure"),
    };

    assert_eq!(owner_deadline(&refused), expected_deadline);
    assert!(data.pending_fatal_for_test().is_none());
    drop((send, refused));
}

#[test]
fn ordinary_route_refusal_faults_with_the_exact_local_context() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let send = data
        .register_pending(record("local-route"), deadline(5))
        .unwrap_or_else(|error| panic!("local route fixture should register: {error:?}"))
        .into_send();
    let rollback = data.host.pending_notifications.begin_startup_rollback();

    let progress = data
        .settle_next_pending(Moment::from_tick(5))
        .unwrap_or_else(|_refused| panic!("first route fault should install"));

    assert_eq!(
        progress.disposition(),
        PendingSettlementDisposition::Faulted
    );
    assert_eq!(data.shard_stats().aggregate_retained_bytes, 0);
    let Some(PendingShardFatal::Notification(notification)) = data.pending_fatal_for_test() else {
        panic!("route refusal should retain one notification owner")
    };
    let PendingNotificationContext::Local(failure) = notification.context_for_test() else {
        panic!("local settlement cannot be reclassified")
    };
    assert_eq!(failure.kind(), ProducerSendFailureKind::DeadlineElapsed);
    assert!(notification.permit_slot_for_test().is_some());
    drop((send, rollback));
}

#[test]
fn invariant_route_refusal_is_the_only_first_fault_owner() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let send = data
        .register_pending(record("accepted-route"), deadline(100))
        .unwrap_or_else(|error| panic!("accepted route fixture should register: {error:?}"))
        .into_send();
    let expected = ProducerHostInvariantError::MissingAdmissionIdentity;
    data.inject_post_acceptance_fault(expected);
    let rollback = data.host.pending_notifications.begin_startup_rollback();

    let progress = data
        .settle_next_pending(Moment::from_tick(1))
        .unwrap_or_else(|_refused| panic!("route context already owns the invariant"));

    assert_eq!(
        progress.disposition(),
        PendingSettlementDisposition::Faulted
    );
    let Some(PendingShardFatal::Notification(notification)) = data.pending_fatal_for_test() else {
        panic!("route refusal should remain the one immutable first owner")
    };
    let PendingNotificationContext::AcceptedInvariant(facts) = notification.context_for_test()
    else {
        panic!("accepted invariant must remain attached to its exact job")
    };
    assert!(facts.operation_id.is_some());
    assert_eq!(
        facts.invariant,
        super::promotion_error::PendingPromotionInvariant::Host(expected)
    );
    assert!(notification.permit_slot_for_test().is_some());
    assert_eq!(data.shard_stats().host.pending_notification_backlog, 0);
    drop((send, rollback));
}

fn fatal_with_deadline(tick: u64) -> (PendingShardFatal, ProducerSend, OperationDeadline) {
    let mut source = ProducerShardData::new(start(valid_limits()));
    let expected_deadline = deadline(tick);
    let registration = source
        .register_pending(record("fatal-owner"), expected_deadline)
        .unwrap_or_else(|error| panic!("fault fixture should register: {error:?}"));
    let send = registration.into_send();
    let take = source
        .pending
        .take_next(1)
        .unwrap_or_else(|error| panic!("fault fixture should claim: {error:?}"));
    let attempt = take
        .into_attempt()
        .unwrap_or_else(|| panic!("live fixture must yield an exact attempt"));
    let failure = PendingPromotionFailure::Detach {
        error: PendingAttemptStateError::Invariant,
        attempt: Box::new(attempt),
    };
    (
        PendingShardFatal::promotion(failure),
        send,
        expected_deadline,
    )
}

fn fatal_deadline(data: &ProducerShardData) -> OperationDeadline {
    data.pending_fatal_for_test().map_or_else(
        || panic!("first fatal owner must remain installed"),
        owner_deadline,
    )
}

fn owner_deadline(owner: &PendingShardFatal) -> OperationDeadline {
    let Some(PendingPromotionFailure::Detach { attempt, .. }) = owner.promotion_for_test() else {
        panic!("test fatal owner should retain its exact detach attempt")
    };
    attempt
        .operation_deadline()
        .unwrap_or_else(|| panic!("retained attempt must keep its deadline"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), Instant::now())
}
