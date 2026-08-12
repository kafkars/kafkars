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
    assert!(producer.idempotence.sequence_leases.contains_key(&route));
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
fn unresolved_partition_leases_a_bounded_consecutive_second_batch() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    producer.install_identity_for_test();
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, 20);
    accumulate(&mut producer, operation_id, batch_id, 1);

    let admitted = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(2),
            deadline: Deadline::from_tick(20),
            record: record(2, 0),
        })
        .unwrap_or_else(|error| panic!("second batch admission failed: {error}"));
    let second_operation = admitted
        .admitted_operation_id()
        .unwrap_or_else(|| panic!("second operation identity"));
    let second_batch = producer
        .operation(second_operation)
        .and_then(crate::ProducerOperation::batch_id)
        .unwrap_or_else(|| panic!("second batch identity"));
    let sealed = accumulate(&mut producer, second_operation, second_batch, 2);

    assert!(sealed.effects().iter().any(|effect| matches!(
        effect,
        ProducerEffect::MaterializeBatch { sequence, .. }
            if sequence.base_sequence() == 1 && sequence.record_count() == 1
    )));
    assert_eq!(
        producer.retained_bytes(),
        BYTES
            .checked_add(BYTES)
            .unwrap_or_else(|| panic!("two test records fit retained bytes"))
    );
    assert_eq!(producer.completion_slots(), 2);
}

#[test]
fn out_of_order_success_advances_only_the_contiguous_sequence_frontier() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    producer.install_identity_for_test();
    let (first_operation, first_batch) = admit(&mut producer, 1, 0, 20);
    accumulate(&mut producer, first_operation, first_batch, 1);
    submit(&mut producer, first_batch);
    let (second_operation, second_batch) = admit(&mut producer, 2, 0, 20);
    accumulate(&mut producer, second_operation, second_batch, 2);
    submit(&mut producer, second_batch);
    let route = BatchRoute {
        topic_id: TOPIC,
        partition: PartitionIndex::from_raw(0),
    };

    producer
        .apply(ProducerInput::BrokerSucceeded {
            execution: execution(second_batch),
            success: ProducerBatchSuccess::new(2, None, None),
        })
        .unwrap_or_else(|error| panic!("second success failed: {error}"));
    assert!(!producer.idempotence.next_sequences.contains_key(&route));
    assert_eq!(producer.idempotence.sequence_leases[&route].len(), 2);

    producer
        .apply(ProducerInput::BrokerSucceeded {
            execution: execution(first_batch),
            success: ProducerBatchSuccess::new(1, None, None),
        })
        .unwrap_or_else(|error| panic!("first success failed: {error}"));
    assert_eq!(producer.idempotence.next_sequences.get(&route), Some(&2));
    assert!(!producer.idempotence.sequence_leases.contains_key(&route));
}

#[test]
fn queued_sequence_leases_use_the_configured_completion_bound() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 6);
    producer.install_identity_for_test();
    for sequence in 0_u64..6 {
        let (operation_id, batch_id) = admit(&mut producer, sequence + 1, 0, 20);
        let sealed = accumulate(&mut producer, operation_id, batch_id, sequence + 1);
        assert!(sealed.effects().iter().any(|effect| matches!(
            effect,
            ProducerEffect::MaterializeBatch { sequence: lease, .. }
                if lease.base_sequence()
                    == i32::try_from(sequence).unwrap_or_else(|_| panic!("sequence fits i32"))
        )));
    }
    assert_eq!(
        producer.apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(7),
            deadline: Deadline::from_tick(20),
            record: record(7, 0),
        }),
        Err(ProducerMachineError::Admission(
            AdmissionRejection::AccumulatorPending,
        ))
    );
    let route = BatchRoute {
        topic_id: TOPIC,
        partition: PartitionIndex::from_raw(0),
    };
    assert_eq!(producer.idempotence.sequence_leases[&route].len(), 6);
}
