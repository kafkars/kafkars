//! One-submit lifecycle, page correlation, order restoration, and delivery tests.

#![expect(
    clippy::expect_used,
    reason = "test fixtures require contextual transition failures"
)]

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeTopicPartitionsCursor, DescribeTopicPartitionsEffect,
    DescribeTopicPartitionsFailureKind, DescribeTopicPartitionsInput,
    DescribeTopicPartitionsMachine, DescribeTopicPartitionsMachineError,
    DescribeTopicPartitionsPage, DescribeTopicPartitionsPlan, DescribeTopicPartitionsState,
    DescribeTopicPartitionsTerminal, page_test::topic, value_test::partition,
};

fn machine(cursor: Option<DescribeTopicPartitionsCursor>) -> DescribeTopicPartitionsMachine {
    DescribeTopicPartitionsMachine::new(
        OperationId::from_raw(7),
        Deadline::from_tick(20),
        DescribeTopicPartitionsPlan::new(
            vec!["zeta".to_owned(), "alpha".to_owned(), "omega".to_owned()],
            3,
            cursor,
        )
        .expect("valid plan"),
    )
}

fn submit(machine: &mut DescribeTopicPartitionsMachine) {
    let effect = machine
        .apply(DescribeTopicPartitionsInput::Start {
            now: Moment::from_tick(1),
        })
        .expect("start")
        .into_effect();
    assert!(matches!(
        effect,
        Some(DescribeTopicPartitionsEffect::Submit {
            operation_id,
            deadline,
            ..
        }) if operation_id == OperationId::from_raw(7)
            && deadline == Deadline::from_tick(20)
    ));
    machine
        .apply(DescribeTopicPartitionsInput::DriverAccepted)
        .expect("accepted");
}

#[test]
fn subset_topics_restore_caller_order_without_reordering_partitions() {
    let mut machine = machine(None);
    submit(&mut machine);
    let next = DescribeTopicPartitionsCursor::new("omega".to_owned(), 4).expect("cursor");
    let page = DescribeTopicPartitionsPage::new(
        11,
        vec![
            topic("alpha", vec![partition(2), partition(0)]),
            topic("zeta", vec![partition(1)]),
        ],
        Some(next),
    )
    .expect("page");
    let effect = machine
        .apply(DescribeTopicPartitionsInput::BrokerResponded { page })
        .expect("terminal")
        .into_effect();
    let Some(DescribeTopicPartitionsEffect::Complete {
        terminal: DescribeTopicPartitionsTerminal::Page(page),
        ..
    }) = effect
    else {
        panic!("page terminal");
    };
    assert_eq!(page.topics()[0].name(), "zeta");
    assert_eq!(page.topics()[1].name(), "alpha");
    assert_eq!(page.topics()[1].partitions()[0].partition_index(), 2);
    assert_eq!(page.topics()[1].partitions()[1].partition_index(), 0);
    assert_eq!(
        page.next_cursor()
            .map(super::model::DescribeTopicPartitionsCursor::topic_name),
        Some("omega")
    );
    assert_eq!(machine.state(), DescribeTopicPartitionsState::Completed);
    assert_eq!(
        machine.apply(DescribeTopicPartitionsInput::InvalidResponse),
        Err(DescribeTopicPartitionsMachineError::AlreadyCompleted)
    );
}

#[test]
fn unrequested_topic_and_request_cursor_regression_are_invalid_response() {
    for page in [
        DescribeTopicPartitionsPage::new(0, vec![topic("unrequested", Vec::new())], None)
            .expect("structural page"),
        DescribeTopicPartitionsPage::new(0, vec![topic("alpha", Vec::new())], None)
            .expect("structural page"),
    ] {
        let cursor = DescribeTopicPartitionsCursor::new("omega".to_owned(), 3).expect("cursor");
        let mut machine = machine(Some(cursor));
        submit(&mut machine);
        let effect = machine
            .apply(DescribeTopicPartitionsInput::BrokerResponded { page })
            .expect("terminal")
            .into_effect();
        assert!(matches!(
            effect,
            Some(DescribeTopicPartitionsEffect::Complete {
                terminal: DescribeTopicPartitionsTerminal::Failed(failure),
                ..
            }) if failure.kind() == DescribeTopicPartitionsFailureKind::InvalidResponse
                && failure.delivery() == DeliveryStatus::PossiblySent
        ));
    }
}

#[test]
fn next_cursor_must_advance_beyond_the_request_and_returned_page() {
    let request_cursor =
        DescribeTopicPartitionsCursor::new("alpha".to_owned(), 3).expect("request cursor");
    for page in [
        DescribeTopicPartitionsPage::new(
            0,
            Vec::new(),
            Some(DescribeTopicPartitionsCursor::new("alpha".to_owned(), 3).expect("same cursor")),
        )
        .expect("structural page"),
        DescribeTopicPartitionsPage::new(
            0,
            vec![topic("alpha", vec![partition(5)])],
            Some(
                DescribeTopicPartitionsCursor::new("alpha".to_owned(), 5).expect("returned cursor"),
            ),
        )
        .expect("structural page"),
    ] {
        let mut machine = machine(Some(request_cursor.clone()));
        submit(&mut machine);
        let effect = machine
            .apply(DescribeTopicPartitionsInput::BrokerResponded { page })
            .expect("terminal")
            .into_effect();
        assert!(matches!(
            effect,
            Some(DescribeTopicPartitionsEffect::Complete {
                terminal: DescribeTopicPartitionsTerminal::Failed(failure),
                ..
            }) if failure.kind() == DescribeTopicPartitionsFailureKind::InvalidResponse
                && failure.delivery() == DeliveryStatus::PossiblySent
        ));
    }
}

#[test]
fn response_partition_limit_is_enforced_against_the_explicit_page() {
    let mut machine = machine(None);
    submit(&mut machine);
    let page = DescribeTopicPartitionsPage::new(
        0,
        vec![topic(
            "zeta",
            vec![partition(0), partition(1), partition(2), partition(3)],
        )],
        None,
    )
    .expect("globally bounded page");
    let effect = machine
        .apply(DescribeTopicPartitionsInput::BrokerResponded { page })
        .expect("terminal")
        .into_effect();
    assert!(matches!(
        effect,
        Some(DescribeTopicPartitionsEffect::Complete {
            terminal: DescribeTopicPartitionsTerminal::Failed(failure),
            ..
        }) if failure.kind() == DescribeTopicPartitionsFailureKind::InvalidResponse
            && failure.delivery() == DeliveryStatus::PossiblySent
    ));
}

#[test]
fn deadline_and_transport_paths_preserve_authoritative_delivery() {
    let mut expired = machine(None);
    let effect = expired
        .apply(DescribeTopicPartitionsInput::Start {
            now: Moment::from_tick(20),
        })
        .expect("expired")
        .into_effect();
    assert!(matches!(
        effect,
        Some(DescribeTopicPartitionsEffect::Complete {
            terminal: DescribeTopicPartitionsTerminal::Failed(failure),
            ..
        }) if failure.kind() == DescribeTopicPartitionsFailureKind::DeadlineElapsed
            && failure.delivery() == DeliveryStatus::NotSent
    ));

    let mut submitted = machine(None);
    submit(&mut submitted);
    let effect = submitted
        .apply(DescribeTopicPartitionsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        })
        .expect("transport")
        .into_effect();
    assert!(matches!(
        effect,
        Some(DescribeTopicPartitionsEffect::Complete {
            terminal: DescribeTopicPartitionsTerminal::Failed(failure),
            ..
        }) if failure.kind() == DescribeTopicPartitionsFailureKind::Transport
            && failure.delivery() == DeliveryStatus::PossiblySent
    ));
}
