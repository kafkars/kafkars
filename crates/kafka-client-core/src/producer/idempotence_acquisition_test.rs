//! Lazy identity acquisition, deadline, and atomic fencing scenarios.

use core::num::NonZeroI16;

use crate::{
    ByteCount, Deadline, DeliveryStatus, Moment, ProducerEffect, ProducerIdentityGeneration,
    ProducerInput, ProducerMachine, ProducerMachineError,
};

use super::scenario_support::idempotence::{BYTES, accumulate, acquire, admit};

#[test]
fn lazy_identity_uses_original_deadline_before_materialization() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, 20);
    let sealed = accumulate(&mut producer, operation_id, batch_id, 1);
    assert!(matches!(
        sealed.effects(),
        [
            ProducerEffect::ArmBatchTimer {
                deadline,
                ..
            },
            ProducerEffect::AcquireProducerIdentity {
                generation,
                deadline_operation_id,
                deadline: acquisition_deadline,
            },
        ] if *deadline == Deadline::from_tick(20)
            && *generation == ProducerIdentityGeneration::initial()
            && *deadline_operation_id == operation_id
            && *acquisition_deadline == Deadline::from_tick(20)
    ));

    let acquired = acquire(&mut producer, 10);
    assert!(matches!(
        acquired.effects(),
        [
            ProducerEffect::CancelBatchTimer { batch_id: cancelled, .. },
            ProducerEffect::MaterializeBatch {
                execution,
                identity,
                sequence,
                ..
            },
        ] if *cancelled == batch_id
            && execution.batch_id() == batch_id
            && identity.producer_id() == 11
            && identity.producer_epoch() == 3
            && sequence.base_sequence() == 0
            && sequence.record_count() == 1
    ));
}

#[test]
fn identity_result_after_public_deadline_never_materializes() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, 5);
    accumulate(&mut producer, operation_id, batch_id, 1);

    let acquired = acquire(&mut producer, 5);

    assert!(
        acquired
            .effects()
            .iter()
            .all(|effect| !matches!(effect, ProducerEffect::MaterializeBatch { .. }))
    );
    assert!(acquired.effects().iter().any(|effect| matches!(
        effect,
        ProducerEffect::Complete {
            operation_id: completed,
            ..
        } if *completed == operation_id
    )));
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
}

#[test]
fn invalid_identity_preflight_has_no_partial_mutation() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, 20);
    accumulate(&mut producer, operation_id, batch_id, 1);

    assert_eq!(
        producer.apply(ProducerInput::ProducerIdentityAcquired {
            generation: ProducerIdentityGeneration::initial(),
            producer_id: -1,
            producer_epoch: 0,
            now: Moment::from_tick(2),
        }),
        Err(ProducerMachineError::InvalidProducerIdentity)
    );
    assert_eq!(
        producer.idempotence.acquisition(),
        Some(ProducerIdentityGeneration::initial())
    );
    assert!(producer.batches.contains_key(&batch_id));
    assert_eq!(producer.retained_bytes(), BYTES);
}

#[test]
fn identity_failure_settles_waiting_and_open_batches_atomically() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let (waiting, waiting_batch) = admit(&mut producer, 1, 0, 20);
    accumulate(&mut producer, waiting, waiting_batch, 1);
    let (open, open_batch) = admit(&mut producer, 2, 1, 30);

    let code = NonZeroI16::new(-47).unwrap_or_else(|| panic!("nonzero code"));
    let failed = producer
        .apply(ProducerInput::ProducerIdentityFailed {
            generation: ProducerIdentityGeneration::initial(),
            broker_code: Some(code),
            now: Moment::from_tick(2),
        })
        .unwrap_or_else(|error| panic!("identity failure failed: {error}"));

    assert!(!producer.admission_is_open());
    assert!(producer.batches.is_empty());
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
    for operation_id in [waiting, open] {
        assert!(failed.effects().iter().any(|effect| matches!(
            effect,
            ProducerEffect::Complete {
                operation_id: completed,
                completion: crate::ProducerCompletion::Failed(failure),
            } if *completed == operation_id
                && failure.kind() == crate::ProducerFailureKind::ProducerIdentity
                && failure.broker_code() == Some(-47)
                && failure.delivery() == DeliveryStatus::NotSent
        )));
    }
    for batch_id in [waiting_batch, open_batch] {
        assert!(failed.effects().iter().any(|effect| matches!(
            effect,
            ProducerEffect::CancelBatchTimer {
                batch_id: cancelled,
                ..
            } if *cancelled == batch_id
        )));
    }
}

#[test]
fn failed_fence_preflight_does_not_partially_settle_or_close() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let (waiting, waiting_batch) = admit(&mut producer, 1, 0, 20);
    accumulate(&mut producer, waiting, waiting_batch, 1);
    let (_open, open_batch) = admit(&mut producer, 2, 1, 30);
    let removed = producer.records.remove(&waiting);
    assert!(removed.is_some());

    assert_eq!(
        producer.apply(ProducerInput::ProducerIdentityFailed {
            generation: ProducerIdentityGeneration::initial(),
            broker_code: None,
            now: Moment::from_tick(2),
        }),
        Err(ProducerMachineError::UnknownOperation)
    );
    assert!(producer.admission_is_open());
    assert_eq!(
        producer.idempotence.acquisition(),
        Some(ProducerIdentityGeneration::initial())
    );
    assert!(producer.batches.contains_key(&waiting_batch));
    assert!(producer.batches.contains_key(&open_batch));
    assert_eq!(producer.retained_bytes(), ByteCount::new(16));
}
