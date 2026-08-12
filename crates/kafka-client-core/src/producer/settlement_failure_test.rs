//! Failed-batch settlement releases its exact sequence lease and retained ownership.

use crate::{ByteCount, ProducerEffect, ProducerInput, ProducerMachine};

use super::scenario_support::idempotence::{accumulate, admit, execution};

#[test]
fn definitely_unsent_materialization_failure_reuses_the_unadvanced_sequence() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    producer.install_identity_for_test();
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, 20);
    accumulate(&mut producer, operation_id, batch_id, 1);

    producer
        .apply(ProducerInput::BatchMaterializationFailed {
            execution: execution(batch_id),
        })
        .unwrap_or_else(|error| panic!("materialization failure settlement failed: {error}"));
    producer
        .apply(ProducerInput::CompletionReclaimed { operation_id })
        .unwrap_or_else(|error| panic!("completion reclaim failed: {error}"));

    let (next_operation, next_batch) = admit(&mut producer, 2, 0, 20);
    let sealed = accumulate(&mut producer, next_operation, next_batch, 2);
    assert!(sealed.effects().iter().any(|effect| matches!(
        effect,
        ProducerEffect::MaterializeBatch { sequence, .. }
            if sequence.base_sequence() == 0 && sequence.record_count() == 1
    )));
}

#[test]
fn definitely_unsent_gap_with_a_dependent_lease_fences_new_admission() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    producer.install_identity_for_test();
    let (first_operation, first_batch) = admit(&mut producer, 1, 0, 20);
    accumulate(&mut producer, first_operation, first_batch, 1);
    let (second_operation, second_batch) = admit(&mut producer, 2, 0, 20);
    accumulate(&mut producer, second_operation, second_batch, 2);

    let failed = producer
        .apply(ProducerInput::BatchMaterializationFailed {
            execution: execution(first_batch),
        })
        .unwrap_or_else(|error| panic!("non-tail materialization failure failed: {error}"));

    assert!(!producer.admission_is_open());
    assert!(failed.effects().iter().any(|effect| matches!(
        effect,
        ProducerEffect::Complete {
            operation_id,
            completion: crate::ProducerCompletion::Failed(_),
        } if *operation_id == first_operation
    )));
    assert!(producer.batches.contains_key(&second_batch));
}
