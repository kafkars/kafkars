//! All-mode ordering and selected-response correlation scenarios.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use crate::admin::group_offsets::{
    GroupOffsetDescription, GroupOffsetOutcome, ListConsumerGroupOffsetTarget,
    ListConsumerGroupOffsetsBatch, ListConsumerGroupOffsetsEffect,
    ListConsumerGroupOffsetsFailureKind, ListConsumerGroupOffsetsInput,
    ListConsumerGroupOffsetsMachine, ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsQuery,
    ListConsumerGroupOffsetsTerminal,
};

#[test]
fn selected_response_requires_and_preserves_exact_caller_order() {
    let mut machine = submitted(selected_plan());
    let terminal = apply_terminal(
        &mut machine,
        ListConsumerGroupOffsetsBatch::new(
            17,
            vec![
                described("orders", 2),
                described("audit", 0),
                described("orders", 1),
            ],
        ),
    );
    let ListConsumerGroupOffsetsTerminal::Offsets(batch) = terminal else {
        panic!("selected offsets expected");
    };

    assert_eq!(batch.throttle_time_ms(), 17);
    assert_eq!(
        batch
            .outcomes()
            .iter()
            .map(|outcome| (outcome.topic(), outcome.partition()))
            .collect::<Vec<_>>(),
        [("orders", 2), ("audit", 0), ("orders", 1)]
    );
}

#[test]
fn selected_response_rejects_missing_extra_duplicate_and_out_of_order_identities() {
    for outcomes in [
        vec![described("orders", 2), described("audit", 0)],
        vec![
            described("orders", 2),
            described("audit", 0),
            described("extra", 1),
        ],
        vec![
            described("orders", 2),
            described("audit", 0),
            described("orders", 2),
        ],
        vec![
            described("orders", 1),
            described("audit", 0),
            described("orders", 2),
        ],
    ] {
        let mut machine = submitted(selected_plan());
        let terminal = apply_terminal(
            &mut machine,
            ListConsumerGroupOffsetsBatch::new(0, outcomes),
        );
        let ListConsumerGroupOffsetsTerminal::Failed(failure) = terminal else {
            panic!("invalid response failure expected");
        };
        assert_eq!(
            (failure.kind(), failure.delivery()),
            (
                ListConsumerGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            )
        );
    }
}

#[test]
fn all_response_retains_canonical_topic_byte_and_partition_order() {
    let plan = ListConsumerGroupOffsetsPlan::new("payments".to_owned(), false)
        .unwrap_or_else(|error| panic!("all plan: {error}"));
    let mut machine = submitted(plan);
    let terminal = apply_terminal(
        &mut machine,
        ListConsumerGroupOffsetsBatch::new(
            0,
            vec![
                described("orders", 2),
                described("évents", 0),
                described("z-events", 0),
                described("audit", 1),
                described("orders", 0),
            ],
        ),
    );
    let ListConsumerGroupOffsetsTerminal::Offsets(batch) = terminal else {
        panic!("all offsets expected");
    };

    assert_eq!(
        batch
            .outcomes()
            .iter()
            .map(|outcome| (outcome.topic(), outcome.partition()))
            .collect::<Vec<_>>(),
        [
            ("audit", 1),
            ("orders", 0),
            ("orders", 2),
            ("z-events", 0),
            ("évents", 0),
        ]
    );
}

fn selected_plan() -> ListConsumerGroupOffsetsPlan {
    let query = ListConsumerGroupOffsetsQuery::selected(
        "payments".to_owned(),
        vec![
            ListConsumerGroupOffsetTarget::new("orders".to_owned(), 2),
            ListConsumerGroupOffsetTarget::new("audit".to_owned(), 0),
            ListConsumerGroupOffsetTarget::new("orders".to_owned(), 1),
        ],
    )
    .unwrap_or_else(|error| panic!("selected query: {error}"));
    ListConsumerGroupOffsetsPlan::from_query(query, true)
}

fn submitted(plan: ListConsumerGroupOffsetsPlan) -> ListConsumerGroupOffsetsMachine {
    let mut machine = ListConsumerGroupOffsetsMachine::new(
        OperationId::from_raw(19),
        Deadline::from_tick(20),
        plan,
    );
    machine
        .apply(ListConsumerGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(ListConsumerGroupOffsetsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn apply_terminal(
    machine: &mut ListConsumerGroupOffsetsMachine,
    batch: ListConsumerGroupOffsetsBatch,
) -> ListConsumerGroupOffsetsTerminal {
    let transition = machine
        .apply(ListConsumerGroupOffsetsInput::BrokerResponded { batch })
        .unwrap_or_else(|error| panic!("settle response: {error}"));
    let Some(ListConsumerGroupOffsetsEffect::Complete { terminal, .. }) = transition.into_effect()
    else {
        panic!("terminal effect expected");
    };
    terminal
}

fn described(topic: &str, partition: i32) -> GroupOffsetOutcome {
    GroupOffsetOutcome::described(
        topic.to_owned(),
        partition,
        GroupOffsetDescription::new(None, None, None),
    )
}
