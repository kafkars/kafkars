//! Shard-turn terminal ownership handoff scenarios.

use kafka_client_core::Deadline;

use crate::producer::{
    ProducerHostInvariantError,
    pending::{PendingAttemptStateError, turn_error::PendingTurnFailureOwnership},
};

use super::{
    pending_fatal::PendingShardFatal,
    pending_local_fatal::{PendingLocalSettlementFatal, PendingLocalSettlementMode},
    promotion_error::PendingPromotionFailure,
    shard_turn_failure::{
        ProducerShardTurnFailure, ProducerShardTurnFailureCause, ProducerShardTurnFailureOwner,
    },
    shard_turn_test::{fixture, input, register},
};

#[test]
fn pending_only_cause_returns_the_exact_local_owner() {
    let mut data = fixture();
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
        .unwrap_or_else(|| panic!("closed shard should refuse a non-drain local fault"));
    let cause =
        ProducerShardTurnFailureCause::Pending(ProducerShardTurnFailureOwner::Local(refused));

    let ProducerShardTurnFailureCause::Pending(pending) = cause else {
        panic!("pending-only cause should remain structural")
    };
    let ProducerShardTurnFailureOwner::Local(refused) = pending else {
        panic!("local owner should remain exact")
    };
    let owner = refused.into_owner();
    assert_eq!(owner.mode(), PendingLocalSettlementMode::Expiry);
    assert_eq!(owner.inspected_for_test(), 7);
    assert_eq!(owner.retained_prefix_for_test(), 3);
}

#[test]
fn structural_future_host_and_pending_representation_preserves_real_owners() {
    let mut data = fixture();
    let (first, first_send) = detached_failure(11);
    data.retain_pending_fatal(PendingShardFatal::promotion(first))
        .unwrap_or_else(|_failure| panic!("first pending fault should install"));
    let (later, later_send) = detached_failure(22);
    let refused = match data.retain_promotion_failure_for_test(later) {
        Err(refused) => refused,
        Ok(_progress) => panic!("later pending fault should return intact"),
    };
    let expected = ProducerHostInvariantError::PendingEffectCapacity;
    assert_eq!(data.host.poison(expected), expected);
    let host_failure = match data.shard_turn(input(1, false, 1, 1)) {
        Err(failure) => failure,
        Ok(_progress) => panic!("poisoned host should require terminal handoff"),
    };
    let combined = ProducerShardTurnFailure::new(
        ProducerShardTurnFailureCause::HostAndPending {
            host: expected,
            pending: ProducerShardTurnFailureOwner::Promotion(refused),
        },
        host_failure.progress(),
    );

    assert_eq!(combined.accepted_invariant(), Some(expected));
    let (progress, cause) = combined.into_parts();
    assert!(progress.terminal_handoff());
    let ProducerShardTurnFailureCause::HostAndPending { host, pending } = cause else {
        panic!("both terminal facts must remain structural")
    };
    assert_eq!(host, expected);
    let ProducerShardTurnFailureOwner::Promotion(refused) = pending else {
        panic!("later promotion owner must remain exact")
    };
    let owner = refused.into_owner();
    let Some(PendingPromotionFailure::Detach { attempt, .. }) = owner.promotion_for_test() else {
        panic!("exact detach owner should survive handoff")
    };
    assert_eq!(
        attempt
            .operation_deadline()
            .map(crate::clock::OperationDeadline::core),
        Some(Deadline::from_tick(22))
    );
    drop((first_send, later_send, owner));
}

fn detached_failure(tick: u64) -> (PendingPromotionFailure, crate::ProducerSend) {
    let mut source = fixture();
    let send = register(&mut source, "detached", tick);
    let take = source
        .pending
        .take_next(1)
        .unwrap_or_else(|error| panic!("failure fixture should claim: {error:?}"));
    let attempt = take
        .into_attempt()
        .unwrap_or_else(|| panic!("failure fixture needs one live attempt"));
    (
        PendingPromotionFailure::Detach {
            error: PendingAttemptStateError::Invariant,
            attempt: Box::new(attempt),
        },
        send,
    )
}
