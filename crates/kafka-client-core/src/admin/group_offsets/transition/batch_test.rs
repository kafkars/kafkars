//! Multi-group re-arming, aggregation, and delivery-certainty scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use crate::admin::group_offsets::{
    GroupOffsetDescription, GroupOffsetOutcome, ListConsumerGroupBatchOutcome,
    ListConsumerGroupOffsetsBatch, ListConsumerGroupOffsetsEffect,
    ListConsumerGroupOffsetsFailureKind, ListConsumerGroupOffsetsInput,
    ListConsumerGroupOffsetsMachine, ListConsumerGroupOffsetsPlan,
    ListConsumerGroupOffsetsTerminal, ListConsumerGroupOffsetsTransition,
};

#[test]
fn batch_rearms_singleton_calls_and_preserves_group_order_under_one_deadline() {
    let plan = ListConsumerGroupOffsetsPlan::new_batch(
        vec!["z-readers".to_owned(), "a-readers".to_owned()],
        true,
    )
    .unwrap_or_else(|error| panic!("valid batch plan: {error}"));
    let mut machine = ListConsumerGroupOffsetsMachine::new(
        OperationId::from_raw(29),
        Deadline::from_tick(50),
        plan,
    );

    let first = machine
        .apply(ListConsumerGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start batch: {error}"));
    assert_submit(first, 29, 50, "z-readers");
    machine
        .apply(ListConsumerGroupOffsetsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept first call: {error}"));

    let second = machine
        .apply(ListConsumerGroupOffsetsInput::BrokerRejected {
            code: nonzero(-719),
            throttle_time_ms: 41,
        })
        .unwrap_or_else(|error| panic!("settle first group: {error}"));
    assert_submit(second, 29, 50, "a-readers");
    machine
        .apply(ListConsumerGroupOffsetsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second call: {error}"));

    let complete = machine
        .apply(ListConsumerGroupOffsetsInput::BrokerResponded {
            batch: ListConsumerGroupOffsetsBatch::new(
                17,
                vec![described("orders", 0, Some(42), None, None)],
            ),
        })
        .unwrap_or_else(|error| panic!("settle second group: {error}"));
    let Some(ListConsumerGroupOffsetsEffect::Complete {
        terminal: ListConsumerGroupOffsetsTerminal::Batch(batch),
        ..
    }) = complete.into_effect()
    else {
        panic!("batch must complete exactly once");
    };

    assert_eq!(batch.throttle_time_ms(), 41);
    assert_eq!(batch.outcomes().len(), 2);
    assert_eq!(batch.outcomes()[0].group_id(), "z-readers");
    assert_eq!(batch.outcomes()[1].group_id(), "a-readers");
    assert!(matches!(
        &batch.outcomes()[0],
        ListConsumerGroupBatchOutcome::BrokerRejected { code, .. }
            if code.get() == -719
    ));
    assert!(matches!(
        &batch.outcomes()[1],
        ListConsumerGroupBatchOutcome::Offsets { offsets, .. }
            if offsets.outcomes().len() == 1
    ));
}

#[test]
fn batch_failure_after_a_settled_group_never_improves_delivery_certainty() {
    let plan = ListConsumerGroupOffsetsPlan::new_batch(
        vec!["first".to_owned(), "second".to_owned()],
        false,
    )
    .unwrap_or_else(|error| panic!("valid batch plan: {error}"));
    let mut machine = ListConsumerGroupOffsetsMachine::new(
        OperationId::from_raw(31),
        Deadline::from_tick(50),
        plan,
    );
    machine
        .apply(ListConsumerGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(ListConsumerGroupOffsetsInput::DriverAccepted))
        .and_then(|_| {
            machine.apply(ListConsumerGroupOffsetsInput::BrokerResponded {
                batch: ListConsumerGroupOffsetsBatch::new(0, Vec::new()),
            })
        })
        .unwrap_or_else(|error| panic!("settle first group: {error}"));

    assert_failure(
        machine
            .apply(ListConsumerGroupOffsetsInput::DeadlineElapsed)
            .unwrap_or_else(|error| panic!("later deadline: {error}")),
        ListConsumerGroupOffsetsFailureKind::DeadlineElapsed,
        DeliveryStatus::PossiblySent,
    );
}

fn described(
    topic: &str,
    partition: i32,
    offset: Option<i64>,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
) -> GroupOffsetOutcome {
    GroupOffsetOutcome::described(
        topic.to_owned(),
        partition,
        GroupOffsetDescription::new(offset, leader_epoch, metadata),
    )
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}

fn assert_failure(
    transition: ListConsumerGroupOffsetsTransition,
    kind: ListConsumerGroupOffsetsFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(ListConsumerGroupOffsetsEffect::Complete {
        terminal: ListConsumerGroupOffsetsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}

fn assert_submit(
    transition: ListConsumerGroupOffsetsTransition,
    operation_id: u64,
    deadline: u64,
    group_id: &str,
) {
    let Some(ListConsumerGroupOffsetsEffect::Submit {
        operation_id: actual_id,
        deadline: actual_deadline,
        plan,
    }) = transition.into_effect()
    else {
        panic!("expected singleton submit");
    };
    assert_eq!(actual_id, OperationId::from_raw(operation_id));
    assert_eq!(actual_deadline, Deadline::from_tick(deadline));
    assert_eq!(plan.group_ids(), [group_id]);
}
