//! Selected-query retained-byte and batched re-arming scenarios.

use std::time::Instant;

use kafka_client_core::{
    ListConsumerGroupOffsetTarget, ListConsumerGroupOffsetsInput, ListConsumerGroupOffsetsPlan,
    ListConsumerGroupOffsetsQuery, ListConsumerGroupOffsetsSelection, Moment,
};

use crate::clock::OperationDeadline;

use super::ListConsumerGroupOffsetsTurn;

#[test]
fn selected_query_charges_retained_selection_and_correlation_scratch() {
    let all_result_limit = admitted_result_limit(all_plan());
    let selected_result_limit = admitted_result_limit(selected_plan());
    assert!(selected_result_limit < all_result_limit);
}

#[test]
fn batch_rearming_preserves_each_groups_exact_selection() {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(50), selected_batch_plan())
        .unwrap_or_else(|error| panic!("admit selected offset batch: {error:?}"));

    let ListConsumerGroupOffsetsTurn::Submit(first) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("first selected submission: {error}"))
    else {
        panic!("first selected submission expected");
    };
    let (operation_id, _, first_plan, _) = first.into_parts();
    assert!(matches!(
        first_plan.selection(),
        ListConsumerGroupOffsetsSelection::Selected(targets)
            if targets.iter().map(|target| (target.topic(), target.partition())).collect::<Vec<_>>()
                == [("orders", 3), ("audit", 1)]
    ));
    settle_rejected(&mut host, operation_id, -719);

    let ListConsumerGroupOffsetsTurn::Submit(second) = host
        .turn(Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("second all submission: {error}"))
    else {
        panic!("second all submission expected");
    };
    let (second_id, _, second_plan, _) = second.into_parts();
    assert_eq!(second_id, operation_id);
    assert!(matches!(
        second_plan.selection(),
        ListConsumerGroupOffsetsSelection::All
    ));
    settle_rejected(&mut host, operation_id, -720);
    let _batch = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe selected batch: {error}"));

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

fn settle_rejected(
    host: &mut super::ListConsumerGroupOffsetsHost,
    operation_id: kafka_client_core::OperationId,
    code: i16,
) {
    host.apply_for_test(
        operation_id,
        ListConsumerGroupOffsetsInput::DriverAccepted,
        0,
    )
    .and_then(|_| {
        host.apply_for_test(
            operation_id,
            ListConsumerGroupOffsetsInput::BrokerRejected {
                code: core::num::NonZeroI16::new(code).unwrap_or_else(|| panic!("nonzero")),
                throttle_time_ms: 0,
            },
            0,
        )
    })
    .unwrap_or_else(|error| panic!("settle selected singleton: {error}"));
}

fn all_plan() -> ListConsumerGroupOffsetsPlan {
    ListConsumerGroupOffsetsPlan::new("payments".to_owned(), true)
        .unwrap_or_else(|error| panic!("valid all query: {error}"))
}

fn selected_plan() -> ListConsumerGroupOffsetsPlan {
    ListConsumerGroupOffsetsPlan::from_query(
        ListConsumerGroupOffsetsQuery::selected(
            "payments".to_owned(),
            vec![
                ListConsumerGroupOffsetTarget::new("orders".to_owned(), 3),
                ListConsumerGroupOffsetTarget::new("audit".to_owned(), 1),
            ],
        )
        .unwrap_or_else(|error| panic!("valid selected query: {error}")),
        true,
    )
}

fn selected_batch_plan() -> ListConsumerGroupOffsetsPlan {
    ListConsumerGroupOffsetsPlan::new_query_batch(
        vec![
            ListConsumerGroupOffsetsQuery::selected(
                "z-readers".to_owned(),
                vec![
                    ListConsumerGroupOffsetTarget::new("orders".to_owned(), 3),
                    ListConsumerGroupOffsetTarget::new("audit".to_owned(), 1),
                ],
            )
            .unwrap_or_else(|error| panic!("valid selected query: {error}")),
            ListConsumerGroupOffsetsQuery::all("a-readers".to_owned())
                .unwrap_or_else(|error| panic!("valid all query: {error}")),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid selected batch plan: {error}"))
}

fn admitted_result_limit(plan: ListConsumerGroupOffsetsPlan) -> usize {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(50), plan)
        .unwrap_or_else(|error| panic!("admit plan: {error:?}"));
    let ListConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take plan submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_, _, _, result_limit) = submission.into_parts();
    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
    result_limit
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}
