//! Admission sequencing and initial accumulator ownership scenarios.

use crate::{
    ByteCount, Deadline, ExplicitRecord, Moment, PartitionIndex, PayloadId, ProducerEffect,
    ProducerInput, ProducerMachine, TopicId,
};

#[test]
fn first_route_admission_reserves_terminal_and_retained_capacity_before_accumulation() {
    let mut producer = ProducerMachine::new(ByteCount::new(16), 1);
    let admitted = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(2),
            deadline: Deadline::from_tick(20),
            record: ExplicitRecord::new(
                PayloadId::from_raw(1),
                TopicId::from_raw(3),
                PartitionIndex::from_raw(4),
                ByteCount::new(8),
            ),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));

    assert!(matches!(
        admitted.effects().first(),
        Some(ProducerEffect::AccumulateExplicit { .. })
    ));
    assert_eq!(producer.retained_bytes(), ByteCount::new(8));
    assert_eq!(producer.completion_slots(), 1);
}
