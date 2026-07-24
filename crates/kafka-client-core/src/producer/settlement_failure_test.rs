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
