//! Scenarios for strict API-91 topic-partition correlation.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ALTER_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES, ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS,
    AlterShareGroupOffset, AlterShareGroupOffsetsBatch, AlterShareGroupOffsetsEffect,
    AlterShareGroupOffsetsFailureKind, AlterShareGroupOffsetsInput, AlterShareGroupOffsetsMachine,
    AlterShareGroupOffsetsPartitionBrokerError, AlterShareGroupOffsetsPartitionOutcome,
    AlterShareGroupOffsetsPartitionResult, AlterShareGroupOffsetsPlan,
    AlterShareGroupOffsetsTerminal,
};

#[test]
fn response_is_restored_to_caller_order_with_exact_partition_facts() {
    let mut machine = submitted();
    let transition = machine
        .apply(AlterShareGroupOffsetsInput::BrokerResponded {
            batch: AlterShareGroupOffsetsBatch::new(
                73,
                vec![
                    AlterShareGroupOffsetsPartitionOutcome::failed(
                        "audit".to_owned(),
                        [8; 16],
                        0,
                        AlterShareGroupOffsetsPartitionBrokerError::new(
                            nonzero(-31_999),
                            Some("rejected".to_owned()),
                            false,
                        ),
                    ),
                    AlterShareGroupOffsetsPartitionOutcome::altered(
                        "orders".to_owned(),
                        [7; 16],
                        1,
                    ),
                ],
            ),
        })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    let AlterShareGroupOffsetsTerminal::Altered(batch) = terminal(transition) else {
        panic!("valid response must be altered");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    assert_eq!(batch.outcomes()[0].topic_id(), [7; 16]);
    assert_eq!(
        batch.outcomes()[0].result(),
        &AlterShareGroupOffsetsPartitionResult::Altered
    );
    assert_eq!(batch.outcomes()[1].topic(), "audit");
}

#[test]
fn missing_duplicate_unexpected_negative_or_zero_id_is_invalid() {
    for outcomes in [
        vec![altered("orders", [1; 16], 1)],
        vec![altered("orders", [1; 16], 1), altered("orders", [2; 16], 1)],
        vec![
            altered("orders", [1; 16], 1),
            altered("unknown", [2; 16], 0),
        ],
        vec![altered("orders", [0; 16], 1), altered("audit", [2; 16], 0)],
        vec![altered("orders", [1; 16], -1), altered("audit", [2; 16], 0)],
    ] {
        assert_failed(
            apply_batch(outcomes),
            AlterShareGroupOffsetsFailureKind::InvalidResponse,
        );
    }
}

#[test]
fn invalid_diagnostic_and_excessive_count_are_bounded_terminals() {
    let invalid = AlterShareGroupOffsetsPartitionBrokerError::new(
        nonzero(1),
        Some("x".repeat(ALTER_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES + 1)),
        false,
    );
    assert_failed(
        apply_batch(vec![
            altered("orders", [1; 16], 1),
            AlterShareGroupOffsetsPartitionOutcome::failed("audit".to_owned(), [2; 16], 0, invalid),
        ]),
        AlterShareGroupOffsetsFailureKind::InvalidResponse,
    );

    let oversized = (0..=ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS)
        .map(|partition| altered("t", [1; 16], partition as i32))
        .collect();
    assert_failed(
        apply_batch(oversized),
        AlterShareGroupOffsetsFailureKind::ResponseTooLarge,
    );
}

fn apply_batch(
    outcomes: Vec<AlterShareGroupOffsetsPartitionOutcome>,
) -> AlterShareGroupOffsetsTerminal {
    let transition = submitted()
        .apply(AlterShareGroupOffsetsInput::BrokerResponded {
            batch: AlterShareGroupOffsetsBatch::new(0, outcomes),
        })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    terminal(transition)
}

fn submitted() -> AlterShareGroupOffsetsMachine {
    let plan = AlterShareGroupOffsetsPlan::new(
        "share-workers".to_owned(),
        vec![change("orders", 1, 42), change("audit", 0, 7)],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let mut machine = AlterShareGroupOffsetsMachine::new(
        OperationId::from_raw(91),
        Deadline::from_tick(20),
        plan,
    );
    machine
        .apply(AlterShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(AlterShareGroupOffsetsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn change(topic: &str, partition: i32, offset: i64) -> AlterShareGroupOffset {
    AlterShareGroupOffset::new(topic.to_owned(), partition, offset)
}

fn altered(
    topic: &str,
    topic_id: [u8; 16],
    partition: i32,
) -> AlterShareGroupOffsetsPartitionOutcome {
    AlterShareGroupOffsetsPartitionOutcome::altered(topic.to_owned(), topic_id, partition)
}

fn terminal(transition: super::AlterShareGroupOffsetsTransition) -> AlterShareGroupOffsetsTerminal {
    let Some(AlterShareGroupOffsetsEffect::Complete { terminal, .. }) = transition.into_effect()
    else {
        panic!("expected terminal effect");
    };
    terminal
}

fn assert_failed(
    terminal: AlterShareGroupOffsetsTerminal,
    kind: AlterShareGroupOffsetsFailureKind,
) {
    let AlterShareGroupOffsetsTerminal::Failed(failure) = terminal else {
        panic!("expected failure");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}
