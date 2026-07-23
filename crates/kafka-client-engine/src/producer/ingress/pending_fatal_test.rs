//! Dormant first-fault retention and terminal-refusal scenarios.

use std::time::Instant;

use kafka_client_core::{AdmissionRejection, Deadline, Moment};

use crate::{
    ProducerSend,
    clock::OperationDeadline,
    producer::{
        ProducerRejectionReason,
        admission::ProducerAdmissionFailure,
        admission_test::record,
        host_limits_test::{start, valid_limits},
        pending::{PendingAdmissionRejectionReason, PendingAttemptStateError},
    },
};

use super::{
    data::ProducerShardData, pending_fatal::PendingShardFatal,
    promotion_error::PendingPromotionFailure, terminal::ProducerShardTerminalError,
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
    (PendingShardFatal::new(failure), send, expected_deadline)
}

fn fatal_deadline(data: &ProducerShardData) -> OperationDeadline {
    data.pending_fatal_for_test().map_or_else(
        || panic!("first fatal owner must remain installed"),
        owner_deadline,
    )
}

fn owner_deadline(owner: &PendingShardFatal) -> OperationDeadline {
    let PendingPromotionFailure::Detach { attempt, .. } = owner.failure_for_test() else {
        panic!("test fatal owner should retain its exact detach attempt")
    };
    attempt
        .operation_deadline()
        .unwrap_or_else(|| panic!("retained attempt must keep its deadline"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), Instant::now())
}
