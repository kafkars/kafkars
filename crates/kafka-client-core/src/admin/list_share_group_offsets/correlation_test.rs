//! Caller correlation, canonical ordering, and hostile API-90 responses.

#![expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::needless_pass_by_value,
    reason = "fixture indices are bounded and helpers preserve terminal ownership"
)]

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    LIST_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES, LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS,
    ListShareGroupOffsetDescription, ListShareGroupOffsetOutcome, ListShareGroupOffsetResult,
    ListShareGroupOffsetTarget, ListShareGroupOffsetsBatch, ListShareGroupOffsetsEffect,
    ListShareGroupOffsetsFailureKind, ListShareGroupOffsetsInput, ListShareGroupOffsetsMachine,
    ListShareGroupOffsetsPartitionBrokerError, ListShareGroupOffsetsPlan,
    ListShareGroupOffsetsTerminal,
};

#[test]
fn selected_response_is_correlated_back_to_exact_caller_order() {
    let mut machine = submitted(selected_plan());
    let batch = ListShareGroupOffsetsBatch::new(
        31,
        vec![
            described("orders", [7; 16], 1, Some(91), Some(4), Some(6)),
            described("orders", [7; 16], 2, Some(42), None, None),
            failed("audit", [8; 16], 0, -29),
        ],
    );
    let ListShareGroupOffsetsTerminal::Offsets(batch) = terminal(apply(
        &mut machine,
        ListShareGroupOffsetsInput::BrokerResponded { batch },
    )) else {
        panic!("offset batch expected");
    };

    assert_eq!(batch.throttle_time_ms(), 31);
    assert_eq!(
        batch
            .outcomes()
            .iter()
            .map(|outcome| (outcome.topic(), outcome.partition()))
            .collect::<Vec<_>>(),
        [("orders", 2), ("audit", 0), ("orders", 1)]
    );
    assert_eq!(batch.outcomes()[0].topic_id(), [7; 16]);
    let ListShareGroupOffsetResult::Described(description) = batch.outcomes()[0].result() else {
        panic!("description expected");
    };
    assert_eq!(description.into_parts(), (Some(42), None, None));
}

#[test]
fn all_response_is_sorted_by_topic_bytes_then_partition() {
    let mut machine = submitted(all_plan());
    let batch = ListShareGroupOffsetsBatch::new(
        0,
        vec![
            described("orders", [7; 16], 2, None, None, None),
            described("audit", [8; 16], 3, None, None, None),
            described("orders", [7; 16], 0, None, None, None),
        ],
    );
    let ListShareGroupOffsetsTerminal::Offsets(batch) = terminal(apply(
        &mut machine,
        ListShareGroupOffsetsInput::BrokerResponded { batch },
    )) else {
        panic!("offset batch expected");
    };

    assert_eq!(
        batch
            .outcomes()
            .iter()
            .map(|outcome| (outcome.topic(), outcome.partition()))
            .collect::<Vec<_>>(),
        [("audit", 3), ("orders", 0), ("orders", 2)]
    );
}

#[test]
fn malformed_selected_identity_topic_id_and_scalar_facts_fail_closed() {
    for batch in [
        ListShareGroupOffsetsBatch::new(
            0,
            vec![
                described("orders", [7; 16], 2, None, None, None),
                described("audit", [8; 16], 0, None, None, None),
            ],
        ),
        ListShareGroupOffsetsBatch::new(
            0,
            vec![
                described("orders", [0; 16], 2, None, None, None),
                described("audit", [8; 16], 0, None, None, None),
                described("orders", [7; 16], 1, None, None, None),
            ],
        ),
        ListShareGroupOffsetsBatch::new(
            0,
            vec![
                described("orders", [7; 16], 2, Some(-1), None, None),
                described("audit", [8; 16], 0, None, None, None),
                described("orders", [7; 16], 1, None, None, None),
            ],
        ),
        ListShareGroupOffsetsBatch::new(
            0,
            vec![
                described("orders", [7; 16], 2, None, None, None),
                described("audit", [8; 16], 0, None, None, None),
                described("orders", [6; 16], 1, None, None, None),
            ],
        ),
    ] {
        let mut machine = submitted(selected_plan());
        assert_invalid(terminal(apply(
            &mut machine,
            ListShareGroupOffsetsInput::BrokerResponded { batch },
        )));
    }
}

#[test]
fn response_count_and_diagnostic_bounds_are_enforced_by_core() {
    let outcomes = (0..=LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS)
        .map(|partition| described("orders", [7; 16], partition as i32, None, None, None))
        .collect();
    let mut machine = submitted(all_plan());
    assert_failure(
        terminal(apply(
            &mut machine,
            ListShareGroupOffsetsInput::BrokerResponded {
                batch: ListShareGroupOffsetsBatch::new(0, outcomes),
            },
        )),
        ListShareGroupOffsetsFailureKind::ResponseTooLarge,
    );

    let error = ListShareGroupOffsetsPartitionBrokerError::new(
        nonzero(-1),
        Some("x".repeat(LIST_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES + 1)),
        true,
    );
    let mut machine = submitted(all_plan());
    assert_invalid(terminal(apply(
        &mut machine,
        ListShareGroupOffsetsInput::BrokerResponded {
            batch: ListShareGroupOffsetsBatch::new(
                0,
                vec![ListShareGroupOffsetOutcome::failed(
                    "orders".to_owned(),
                    [7; 16],
                    0,
                    error,
                )],
            ),
        },
    )));
}

fn selected_plan() -> ListShareGroupOffsetsPlan {
    ListShareGroupOffsetsPlan::selected(
        "share-workers".to_owned(),
        vec![
            ListShareGroupOffsetTarget::new("orders".to_owned(), 2),
            ListShareGroupOffsetTarget::new("audit".to_owned(), 0),
            ListShareGroupOffsetTarget::new("orders".to_owned(), 1),
        ],
    )
    .unwrap_or_else(|error| panic!("selected plan: {error}"))
}

fn all_plan() -> ListShareGroupOffsetsPlan {
    ListShareGroupOffsetsPlan::all("share-workers".to_owned())
        .unwrap_or_else(|error| panic!("all plan: {error}"))
}

fn submitted(plan: ListShareGroupOffsetsPlan) -> ListShareGroupOffsetsMachine {
    let mut machine =
        ListShareGroupOffsetsMachine::new(OperationId::from_raw(90), Deadline::from_tick(20), plan);
    let _submit = apply(
        &mut machine,
        ListShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
        .apply(ListShareGroupOffsetsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept driver: {error}"));
    machine
}

fn apply(
    machine: &mut ListShareGroupOffsetsMachine,
    input: ListShareGroupOffsetsInput,
) -> ListShareGroupOffsetsEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("apply input: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect expected"))
}

fn terminal(effect: ListShareGroupOffsetsEffect) -> ListShareGroupOffsetsTerminal {
    let ListShareGroupOffsetsEffect::Complete { terminal, .. } = effect else {
        panic!("terminal effect expected");
    };
    terminal
}

fn assert_invalid(terminal: ListShareGroupOffsetsTerminal) {
    assert_failure(terminal, ListShareGroupOffsetsFailureKind::InvalidResponse);
}

fn assert_failure(terminal: ListShareGroupOffsetsTerminal, kind: ListShareGroupOffsetsFailureKind) {
    let ListShareGroupOffsetsTerminal::Failed(failure) = terminal else {
        panic!("mechanism failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (kind, DeliveryStatus::PossiblySent)
    );
}

fn described(
    topic: &str,
    topic_id: [u8; 16],
    partition: i32,
    start_offset: Option<i64>,
    leader_epoch: Option<i32>,
    lag: Option<i64>,
) -> ListShareGroupOffsetOutcome {
    ListShareGroupOffsetOutcome::described(
        topic.to_owned(),
        topic_id,
        partition,
        ListShareGroupOffsetDescription::new(start_offset, leader_epoch, lag),
    )
}

fn failed(
    topic: &str,
    topic_id: [u8; 16],
    partition: i32,
    code: i16,
) -> ListShareGroupOffsetOutcome {
    ListShareGroupOffsetOutcome::failed(
        topic.to_owned(),
        topic_id,
        partition,
        ListShareGroupOffsetsPartitionBrokerError::new(nonzero(code), None, false),
    )
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}
