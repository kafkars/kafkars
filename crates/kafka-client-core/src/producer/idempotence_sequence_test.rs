//! Sequence wrap, terminal preflight, and unresolved-route scenarios.

use crate::{
    AdmissionRejection, ByteCount, Deadline, Moment, PartitionIndex, ProducerBatchSuccess,
    ProducerEffect, ProducerInput, ProducerMachine, ProducerMachineError, ProducerSequenceLease,
};

use super::BatchRoute;
use super::scenario_support::idempotence::{
    BYTES, TOPIC, accumulate, admit, execution, record, submit,
};

#[test]
fn successful_sequence_wraps_only_after_terminal_preflight() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    producer.install_identity_for_test();
    let route = BatchRoute {
        topic_id: TOPIC,
        partition: PartitionIndex::from_raw(0),
    };
    producer.idempotence.next_sequences.insert(route, i32::MAX);
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, 20);
    let sealed = accumulate(&mut producer, operation_id, batch_id, 1);
    assert!(sealed.effects().iter().any(|effect| matches!(
        effect,
        ProducerEffect::MaterializeBatch { sequence, .. }
            if *sequence == ProducerSequenceLease::try_new(i32::MAX, 1)
                .unwrap_or_else(|| panic!("valid maximum sequence"))
    )));
    submit(&mut producer, batch_id);
    producer
        .apply(ProducerInput::BrokerSucceeded {
            execution: execution(batch_id),
            success: ProducerBatchSuccess::new(1, None, None),
        })
        .unwrap_or_else(|error| panic!("success failed: {error}"));
    assert_eq!(producer.idempotence.next_sequences.get(&route), Some(&0));
}

#[test]
fn broker_success_preflight_failure_does_not_advance_sequence() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    producer.install_identity_for_test();
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, 20);
    accumulate(&mut producer, operation_id, batch_id, 1);
    submit(&mut producer, batch_id);
    let removed = producer.records.remove(&operation_id);
    assert!(removed.is_some());

    assert_eq!(
        producer.apply(ProducerInput::BrokerSucceeded {
            execution: execution(batch_id),
            success: ProducerBatchSuccess::new(1, None, None),
        }),
        Err(ProducerMachineError::UnknownOperation)
    );
    let route = BatchRoute {
        topic_id: TOPIC,
        partition: PartitionIndex::from_raw(0),
    };
    assert!(!producer.idempotence.next_sequences.contains_key(&route));
    assert!(producer.idempotence.leased_partitions.contains(&route));
}

#[test]
fn missing_sequence_lease_rejects_success_without_any_machine_mutation() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    producer.install_identity_for_test();
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, 20);
    accumulate(&mut producer, operation_id, batch_id, 1);
    submit(&mut producer, batch_id);
    producer
        .batches
        .get_mut(&batch_id)
        .unwrap_or_else(|| panic!("submitted batch must remain"))
        .sequence_lease = None;
    let before = format!("{producer:?}");

    assert_eq!(
        producer.apply(ProducerInput::BrokerSucceeded {
            execution: execution(batch_id),
            success: ProducerBatchSuccess::new(1, None, None),
        }),
        Err(ProducerMachineError::ProducerIdentityFenced)
    );
    assert_eq!(format!("{producer:?}"), before);
}

#[test]
fn unresolved_partition_rejects_second_batch_without_mutation() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    producer.install_identity_for_test();
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, 20);
    accumulate(&mut producer, operation_id, batch_id, 1);

    assert_eq!(
        producer.apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(2),
            deadline: Deadline::from_tick(20),
            record: record(2, 0),
        }),
        Err(ProducerMachineError::Admission(
            AdmissionRejection::AccumulatorPending,
        ))
    );
    assert_eq!(producer.retained_bytes(), BYTES);
    assert_eq!(producer.completion_slots(), 1);
}
