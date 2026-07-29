//! Success and response-validation scenarios for transaction description.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminDescribeTransactionBrokerError, AdminDescribeTransactionDescription,
    AdminDescribeTransactionOutcome, AdminDescribeTransactionResult, AdminDescribeTransactionTopic,
    AdminDescribeTransactionsEffect, AdminDescribeTransactionsFailureKind,
    AdminDescribeTransactionsInput, AdminDescribeTransactionsMachine,
    AdminDescribeTransactionsMachineError, AdminDescribeTransactionsPlan,
    AdminDescribeTransactionsState, AdminDescribeTransactionsTerminal,
    AdminDescribeTransactionsTransition, DESCRIBE_TRANSACTIONS_MAX_PARTITIONS,
    DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES, DESCRIBE_TRANSACTIONS_MAX_TOPIC_BYTES,
    DESCRIBE_TRANSACTIONS_MAX_TOPICS,
};

#[test]
fn each_id_uses_the_original_deadline_and_results_restore_caller_order() {
    let mut machine = two_id_machine(20);
    assert_submit(start(&mut machine), "invoice-worker");
    machine
        .apply(AdminDescribeTransactionsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept first: {error}"));
    let second = machine
        .apply(AdminDescribeTransactionsInput::BrokerResponded {
            throttle_time_ms: 17,
            outcome: described(
                "invoice-worker",
                vec![topic("orders", vec![2, 0]), topic("audit", vec![3, 1])],
            ),
        })
        .unwrap_or_else(|error| panic!("first response: {error}"));
    assert_submit(second, "audit-writer");

    machine
        .apply(AdminDescribeTransactionsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second: {error}"));
    let code = NonZeroI16::new(-31_777).unwrap_or_else(|| panic!("nonzero code"));
    let completed = machine
        .apply(AdminDescribeTransactionsInput::BrokerResponded {
            throttle_time_ms: 73,
            outcome: AdminDescribeTransactionOutcome::broker_failed(
                "audit-writer".to_owned(),
                AdminDescribeTransactionBrokerError::new(code),
            ),
        })
        .unwrap_or_else(|error| panic!("second response: {error}"));
    let Some(AdminDescribeTransactionsEffect::Complete {
        terminal: AdminDescribeTransactionsTerminal::Described(batch),
        ..
    }) = completed.into_effect()
    else {
        panic!("second response must complete");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].transactional_id(), "invoice-worker");
    let AdminDescribeTransactionResult::Described(description) = batch.outcomes()[0].result()
    else {
        panic!("first ID must be described");
    };
    assert_eq!(description.topics()[0].topic(), "audit");
    assert_eq!(description.topics()[0].partitions(), [1, 3]);
    assert_eq!(description.topics()[1].topic(), "orders");
    assert_eq!(description.topics()[1].partitions(), [0, 2]);
    let AdminDescribeTransactionResult::BrokerFailed(error) = batch.outcomes()[1].result() else {
        panic!("second ID must retain broker failure");
    };
    assert_eq!(error.code(), -31_777);
    assert_eq!(machine.state(), AdminDescribeTransactionsState::Completed);
    assert_eq!(
        machine.apply(AdminDescribeTransactionsInput::InvalidResponse),
        Err(AdminDescribeTransactionsMachineError::AlreadyCompleted)
    );
}

#[test]
fn mismatched_and_malformed_nested_facts_are_invalid_responses() {
    assert_invalid_response(described("other", Vec::new()));
    assert_invalid_response(AdminDescribeTransactionOutcome::described(
        "invoice-worker".to_owned(),
        description(String::new(), None, Vec::new()),
    ));
    assert_invalid_response(AdminDescribeTransactionOutcome::described(
        "invoice-worker".to_owned(),
        description(
            "x".repeat(DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES + 1),
            None,
            Vec::new(),
        ),
    ));
    assert_invalid_response(AdminDescribeTransactionOutcome::described(
        "invoice-worker".to_owned(),
        description("Ongoing".to_owned(), Some(-1), Vec::new()),
    ));
    for topics in [
        vec![topic("", vec![0])],
        vec![topic("orders", Vec::new())],
        vec![topic("orders", vec![-1])],
        vec![topic("orders", vec![1, 1])],
        vec![topic("orders", vec![0]), topic("orders", vec![1])],
    ] {
        assert_invalid_response(described("invoice-worker", topics));
    }
}

#[test]
fn aggregate_topic_partition_and_topic_byte_limits_are_enforced() {
    assert_invalid_response(described(
        "invoice-worker",
        vec![topic(
            "orders",
            vec![0; DESCRIBE_TRANSACTIONS_MAX_PARTITIONS + 1],
        )],
    ));
    assert_invalid_response(described(
        "invoice-worker",
        (0..=DESCRIBE_TRANSACTIONS_MAX_TOPICS)
            .map(|index| topic(&format!("topic-{index}"), vec![0]))
            .collect(),
    ));

    let topic_count = DESCRIBE_TRANSACTIONS_MAX_TOPIC_BYTES / 249 + 1;
    let topics = (0..topic_count)
        .map(|index| {
            let prefix = format!("{index:07}-");
            topic(
                &format!("{prefix}{}", "x".repeat(249 - prefix.len())),
                vec![0],
            )
        })
        .collect();
    assert_invalid_response(described("invoice-worker", topics));
}

fn assert_invalid_response(outcome: AdminDescribeTransactionOutcome) {
    let mut machine = one_id_machine(20);
    start(&mut machine);
    machine
        .apply(AdminDescribeTransactionsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept call: {error}"));
    let transition = machine
        .apply(AdminDescribeTransactionsInput::BrokerResponded {
            throttle_time_ms: 0,
            outcome,
        })
        .unwrap_or_else(|error| panic!("invalid response settles: {error}"));
    assert_failure(
        transition,
        AdminDescribeTransactionsFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
}

fn described(
    transactional_id: &str,
    topics: Vec<AdminDescribeTransactionTopic>,
) -> AdminDescribeTransactionOutcome {
    AdminDescribeTransactionOutcome::described(
        transactional_id.to_owned(),
        description("Ongoing".to_owned(), Some(1_700_000_000_123), topics),
    )
}

fn description(
    state: String,
    start: Option<i64>,
    topics: Vec<AdminDescribeTransactionTopic>,
) -> AdminDescribeTransactionDescription {
    AdminDescribeTransactionDescription::new(state, 60_000, start, 91, 7, topics)
}

fn topic(topic: &str, partitions: Vec<i32>) -> AdminDescribeTransactionTopic {
    AdminDescribeTransactionTopic::new(topic.to_owned(), partitions)
}

fn two_id_machine(deadline: u64) -> AdminDescribeTransactionsMachine {
    machine(
        deadline,
        vec!["invoice-worker".to_owned(), "audit-writer".to_owned()],
    )
}

fn one_id_machine(deadline: u64) -> AdminDescribeTransactionsMachine {
    machine(deadline, vec!["invoice-worker".to_owned()])
}

fn machine(deadline: u64, ids: Vec<String>) -> AdminDescribeTransactionsMachine {
    AdminDescribeTransactionsMachine::new(
        OperationId::from_raw(31),
        Deadline::from_tick(deadline),
        AdminDescribeTransactionsPlan::new(ids)
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}

fn start(machine: &mut AdminDescribeTransactionsMachine) -> AdminDescribeTransactionsTransition {
    machine
        .apply(AdminDescribeTransactionsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"))
}

fn assert_submit(transition: AdminDescribeTransactionsTransition, transactional_id: &str) {
    let Some(AdminDescribeTransactionsEffect::Submit {
        operation_id,
        deadline,
        transactional_id: submitted_id,
    }) = transition.into_effect()
    else {
        panic!("expected submit effect");
    };
    assert_eq!(operation_id, OperationId::from_raw(31));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(submitted_id, transactional_id);
}

fn assert_failure(
    transition: AdminDescribeTransactionsTransition,
    kind: AdminDescribeTransactionsFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(AdminDescribeTransactionsEffect::Complete {
        terminal: AdminDescribeTransactionsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
