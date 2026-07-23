//! Removal-plan generation fencing and replay-resistance scenarios.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{Deadline, PartitionIndex};

use super::{
    PendingAdmissionRegistry, PendingPromotionAttempt, PendingRegistryError, ProducerSendFailure,
    ProducerSendFailureKind,
};
use crate::{clock::OperationDeadline, producer::ProducerRecord};

#[test]
fn stale_removal_plan_cannot_take_a_reused_slot() {
    let mut registry = PendingAdmissionRegistry::new(1, 64, 1);
    let old = register(&mut registry, "old");
    let old_id = old.id();
    let old_send = old.into_send();
    let stale_plan = registry
        .validate_remove(old_id)
        .unwrap_or_else(|error| panic!("old removal should validate: {error:?}"));
    let fresh_plan = registry
        .validate_remove(old_id)
        .unwrap_or_else(|error| panic!("second old removal should validate: {error:?}"));
    let promotion = registry.slots[old_id.slot()]
        .entry
        .as_ref()
        .unwrap_or_else(|| panic!("old admission should remain live"))
        .begin_promotion()
        .unwrap_or_else(|error| panic!("old cell should claim: {error:?}"));
    let admission = registry
        .commit_remove(fresh_plan)
        .unwrap_or_else(|failure| panic!("fresh removal should commit: {:?}", failure.error()));
    settle(PendingPromotionAttempt::new(admission, promotion), old_send);

    let live = register(&mut registry, "live");
    let live_id = live.id();
    let live_send = live.into_send();
    assert_ne!(old_id, live_id);
    let failure = registry
        .commit_remove(stale_plan)
        .err()
        .unwrap_or_else(|| panic!("stale proof must not remove a reused slot"));
    assert_eq!(failure.error(), PendingRegistryError::StaleGeneration);
    assert_eq!(registry.stats().records, 1);
    let attempt = registry
        .take_next(1)
        .unwrap_or_else(|error| panic!("live take should remain valid: {error:?}"))
        .into_attempt()
        .unwrap_or_else(|| panic!("live admission should remain queued"));
    assert_eq!(
        attempt
            .retained_admission_for_test()
            .unwrap_or_else(|| panic!("live attempt should retain its record"))
            .topic_for_test(),
        "live"
    );
    settle(attempt, live_send);
}

fn register(
    registry: &mut PendingAdmissionRegistry,
    topic: &str,
) -> super::PendingSendRegistration {
    registry
        .register(
            ProducerRecord::new(
                Arc::from(topic),
                PartitionIndex::from_raw(0),
                1,
                None,
                Some(Bytes::from_static(b"value")),
            ),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(10), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("pending registration should succeed: {error:?}"))
}

fn settle(attempt: PendingPromotionAttempt, send: crate::ProducerSend) {
    let local = attempt
        .settle_local(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|_failure| panic!("pending attempt should settle"));
    let (_admission, job) = local.into_parts();
    job.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
}
