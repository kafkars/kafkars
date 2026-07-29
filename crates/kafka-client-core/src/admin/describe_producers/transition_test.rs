//! Success and response-validation scenarios for active-producer description.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminDescribeProducerBrokerError, AdminDescribeProducerOutcome, AdminDescribeProducerResult,
    AdminDescribeProducerTarget, AdminDescribeProducersEffect, AdminDescribeProducersFailureKind,
    AdminDescribeProducersInput, AdminDescribeProducersMachine, AdminDescribeProducersMachineError,
    AdminDescribeProducersPlan, AdminDescribeProducersState, AdminDescribeProducersTerminal,
    AdminDescribeProducersTransition, AdminProducerState, DESCRIBE_PRODUCERS_DIAGNOSTIC_BYTES,
    DESCRIBE_PRODUCERS_MAX_PRODUCER_STATES,
};

#[test]
fn each_target_uses_the_original_deadline_and_results_restore_caller_order() {
    let mut machine = two_target_machine(20);
    assert_submit(start(&mut machine), "orders", 2);
    machine
        .apply(AdminDescribeProducersInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept first: {error}"));
    let second = machine
        .apply(AdminDescribeProducersInput::BrokerResponded {
            throttle_time_ms: 17,
            outcome: AdminDescribeProducerOutcome::described(
                "orders".to_owned(),
                2,
                vec![producer(91), producer(7)],
            ),
        })
        .unwrap_or_else(|error| panic!("first response: {error}"));
    assert_submit(second, "audit", 0);

    machine
        .apply(AdminDescribeProducersInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second: {error}"));
    let code = NonZeroI16::new(-31_777).unwrap_or_else(|| panic!("nonzero code"));
    let completed = machine
        .apply(AdminDescribeProducersInput::BrokerResponded {
            throttle_time_ms: 73,
            outcome: AdminDescribeProducerOutcome::broker_failed(
                "audit".to_owned(),
                0,
                AdminDescribeProducerBrokerError::new(code, None, false),
            ),
        })
        .unwrap_or_else(|error| panic!("second response: {error}"));
    let Some(AdminDescribeProducersEffect::Complete {
        terminal: AdminDescribeProducersTerminal::Described(batch),
        ..
    }) = completed.into_effect()
    else {
        panic!("second response must complete");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    let AdminDescribeProducerResult::Described(producers) = batch.outcomes()[0].result() else {
        panic!("first target must be described");
    };
    assert_eq!(producers[0].producer_id(), 7);
    assert_eq!(producers[1].producer_id(), 91);
    let AdminDescribeProducerResult::BrokerFailed(error) = batch.outcomes()[1].result() else {
        panic!("second target must retain broker failure");
    };
    assert_eq!(error.code(), -31_777);
    assert_eq!(machine.state(), AdminDescribeProducersState::Completed);
    assert_eq!(
        machine.apply(AdminDescribeProducersInput::InvalidResponse),
        Err(AdminDescribeProducersMachineError::AlreadyCompleted)
    );
}

#[test]
fn each_submit_effect_retains_the_one_exact_broker_selection() {
    let mut machine = AdminDescribeProducersMachine::new(
        OperationId::from_raw(23),
        Deadline::from_tick(20),
        AdminDescribeProducersPlan::new(
            vec![AdminDescribeProducerTarget::new("orders".to_owned(), 2)],
            Some(7),
        )
        .unwrap_or_else(|error| panic!("valid exact-broker plan: {error}")),
    );
    let transition = start(&mut machine);
    let Some(AdminDescribeProducersEffect::Submit { broker_id, .. }) = transition.into_effect()
    else {
        panic!("expected submit effect");
    };
    assert_eq!(broker_id, Some(7));
}

#[test]
fn mismatched_malformed_duplicate_and_over_budget_facts_are_invalid_responses() {
    assert_invalid_response(AdminDescribeProducerOutcome::described(
        "other".to_owned(),
        2,
        Vec::new(),
    ));
    assert_invalid_response(AdminDescribeProducerOutcome::described(
        "orders".to_owned(),
        2,
        vec![AdminProducerState::new(-1, 0, -1, -1, 0, None)],
    ));
    assert_invalid_response(AdminDescribeProducerOutcome::described(
        "orders".to_owned(),
        2,
        vec![producer(7), producer(7)],
    ));

    let code = NonZeroI16::new(1).unwrap_or_else(|| panic!("nonzero code"));
    assert_invalid_response(AdminDescribeProducerOutcome::broker_failed(
        "orders".to_owned(),
        2,
        AdminDescribeProducerBrokerError::new(
            code,
            Some("x".repeat(DESCRIBE_PRODUCERS_DIAGNOSTIC_BYTES + 1)),
            false,
        ),
    ));

    assert_invalid_response(AdminDescribeProducerOutcome::described(
        "orders".to_owned(),
        2,
        vec![producer(7); DESCRIBE_PRODUCERS_MAX_PRODUCER_STATES + 1],
    ));
}

fn assert_invalid_response(outcome: AdminDescribeProducerOutcome) {
    let mut machine = one_target_machine(20);
    start(&mut machine);
    machine
        .apply(AdminDescribeProducersInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept call: {error}"));
    let transition = machine
        .apply(AdminDescribeProducersInput::BrokerResponded {
            throttle_time_ms: 0,
            outcome,
        })
        .unwrap_or_else(|error| panic!("invalid response settles: {error}"));
    assert_failure(
        transition,
        AdminDescribeProducersFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
}

fn two_target_machine(deadline: u64) -> AdminDescribeProducersMachine {
    machine(
        deadline,
        vec![
            AdminDescribeProducerTarget::new("orders".to_owned(), 2),
            AdminDescribeProducerTarget::new("audit".to_owned(), 0),
        ],
    )
}

fn one_target_machine(deadline: u64) -> AdminDescribeProducersMachine {
    machine(
        deadline,
        vec![AdminDescribeProducerTarget::new("orders".to_owned(), 2)],
    )
}

fn machine(
    deadline: u64,
    targets: Vec<AdminDescribeProducerTarget>,
) -> AdminDescribeProducersMachine {
    AdminDescribeProducersMachine::new(
        OperationId::from_raw(23),
        Deadline::from_tick(deadline),
        AdminDescribeProducersPlan::new(targets, None)
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}

fn producer(producer_id: i64) -> AdminProducerState {
    AdminProducerState::new(producer_id, 1, 2, 3, 4, Some(5))
}

fn start(machine: &mut AdminDescribeProducersMachine) -> AdminDescribeProducersTransition {
    machine
        .apply(AdminDescribeProducersInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"))
}

fn assert_submit(transition: AdminDescribeProducersTransition, topic: &str, partition: i32) {
    let Some(AdminDescribeProducersEffect::Submit {
        operation_id,
        deadline,
        target,
        broker_id,
    }) = transition.into_effect()
    else {
        panic!("expected submit effect");
    };
    assert_eq!(operation_id, OperationId::from_raw(23));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(broker_id, None);
    assert_eq!(target.topic(), topic);
    assert_eq!(target.partition(), partition);
}

fn assert_failure(
    transition: AdminDescribeProducersTransition,
    kind: AdminDescribeProducersFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(AdminDescribeProducersEffect::Complete {
        terminal: AdminDescribeProducersTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
