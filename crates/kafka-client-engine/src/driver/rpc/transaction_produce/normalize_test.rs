//! Closed transactional Produce response and certainty classification evidence.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use kafka_client_core::{
    DeliveryStatus, OperationId, ProducerAttemptFailureKind, ProducerBatchSuccess,
    TransactionEndOutcome, TransactionEpoch, TransactionLifecycleEffect, TransactionLifecycleInput,
    TransactionLifecycleMachine, TransactionSendAttempt, TransactionSendId, TransactionalOwnerId,
};
use kafka_driver::{CallFailure, Delivery, RequestError};
use kafka_wire::{
    ProduceResponse,
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
};

use crate::protocol::produce_response::ProduceResponseProtocolFailure;

use super::{
    model::{RouteEvidence, TransactionProduceFailureKind, TransactionProduceTerminalFact},
    normalize::{TransactionProduceResult, normalize_terminal},
};

const TOPIC: &str = "orders";
const PARTITION: i32 = 3;

#[test]
fn success_preserves_exact_epoch_send_and_acknowledgment() {
    let terminal = terminal(TransactionProduceResult::Response(response(0)));
    assert_eq!(
        terminal.fact(),
        TransactionProduceTerminalFact::Succeeded {
            epoch: epoch(),
            send_id: send(),
            success: ProducerBatchSuccess::new(42, None, None),
        }
    );
    assert_correlation(&terminal);
}

#[test]
fn definitely_non_appended_broker_rejection_requires_abort() {
    let terminal = terminal(TransactionProduceResult::Response(response(29)));
    let TransactionProduceTerminalFact::AbortRequired { failure, .. } = terminal.fact() else {
        panic!("authorization rejection must require abort");
    };
    assert_eq!(failure.broker_code(), Some(29));
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn fencing_and_identity_loss_are_fatal_with_exact_codes() {
    for code in [45, 47, 59, 90] {
        let terminal = terminal(TransactionProduceResult::Response(response(code)));
        let TransactionProduceTerminalFact::Fatal { failure, .. } = terminal.fact() else {
            panic!("identity or fencing code {code} must be fatal");
        };
        assert_eq!(failure.broker_code(), Some(code));
        assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    }
}

#[test]
fn append_uncertainty_broker_codes_are_fatal() {
    for code in [7, 13, 20, 56] {
        assert!(matches!(
            terminal(TransactionProduceResult::Response(response(code))).fact(),
            TransactionProduceTerminalFact::Fatal { .. }
        ));
    }
}

#[test]
fn protocol_mismatch_is_possibly_sent_and_fatal() {
    let terminal = terminal(TransactionProduceResult::Response(response_for("other", 0)));
    let TransactionProduceTerminalFact::Fatal { failure, .. } = terminal.fact() else {
        panic!("uncorrelatable response must be fatal");
    };
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    assert_eq!(
        failure.kind(),
        TransactionProduceFailureKind::Protocol(ProduceResponseProtocolFailure::TopicNameMismatch)
    );
}

#[test]
fn driver_certainty_separates_abort_required_from_fatal() {
    let not_sent = RequestError::Rejected {
        failure: CallFailure::LocallyRejected,
        delivery: Delivery::NotSent,
    };
    let possibly_sent = RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::PossiblySent,
    };

    let abort = terminal(TransactionProduceResult::Driver(not_sent));
    let TransactionProduceTerminalFact::AbortRequired { failure, .. } = abort.fact() else {
        panic!("driver-proven NotSent must preserve a healthy identity");
    };
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
    assert_eq!(
        failure.kind(),
        TransactionProduceFailureKind::Driver(ProducerAttemptFailureKind::LocalCapacity)
    );

    let fatal = terminal(TransactionProduceResult::Driver(possibly_sent));
    let TransactionProduceTerminalFact::Fatal { failure, .. } = fatal.fact() else {
        panic!("transport ambiguity must fence sequence identity");
    };
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn route_evidence_is_discarded_only_by_linear_terminal_consumption() {
    let discarded = Arc::new(AtomicBool::new(false));
    let terminal = normalize_terminal(
        epoch(),
        send(),
        Arc::from(TOPIC),
        PARTITION,
        TransactionSendAttempt::initial(),
        TransactionProduceResult::Response(response(0)),
        RouteEvidence::Test(Arc::clone(&discarded)),
    );

    let _fact = terminal.fact();
    assert!(!discarded.load(Ordering::Acquire));
    terminal.discard();
    assert!(discarded.load(Ordering::Acquire));
}

fn terminal(result: TransactionProduceResult) -> super::model::TransactionProduceTerminal {
    normalize_terminal(
        epoch(),
        send(),
        Arc::from(TOPIC),
        PARTITION,
        TransactionSendAttempt::initial(),
        result,
        RouteEvidence::driver(None),
    )
}

fn assert_correlation(terminal: &super::model::TransactionProduceTerminal) {
    assert_eq!(terminal.epoch(), epoch());
    assert_eq!(terminal.send_id(), send());
    assert_eq!(terminal.attempt(), TransactionSendAttempt::initial());
    assert_eq!(terminal.topic().as_ref(), TOPIC);
    assert_eq!(terminal.partition(), PARTITION);
}

fn epoch() -> kafka_client_core::TransactionEpoch {
    transaction_epoch(7)
}

fn send() -> TransactionSendId {
    TransactionSendId::from_raw(11)
}

fn response(code: i16) -> ProduceResponse {
    response_for(TOPIC, code)
}

fn response_for(topic: &str, code: i16) -> ProduceResponse {
    let mut partition = PartitionProduceResponse::default();
    partition.index = PARTITION;
    partition.base_offset = 42;
    partition.error_code = code;
    let mut topic_response = TopicProduceResponse::default();
    topic_response.name = topic.into();
    topic_response.partition_responses.push(partition);
    let mut response = ProduceResponse::default();
    response.responses.push(topic_response);
    response
}

fn transaction_epoch(target: u64) -> TransactionEpoch {
    let owner = TransactionalOwnerId::from_raw(1);
    let mut machine = TransactionLifecycleMachine::new(owner);
    for current in 1..=target {
        let transition = machine
            .apply(owner, TransactionLifecycleInput::Begin)
            .unwrap_or_else(|error| panic!("begin transaction {current}: {error:?}"));
        let Some(TransactionLifecycleEffect::Began { epoch, .. }) = transition.into_effect() else {
            panic!("begin transaction {current} must allocate an epoch");
        };
        if current == target {
            return epoch;
        }
        machine
            .apply(
                owner,
                TransactionLifecycleInput::Abort {
                    epoch,
                    operation_id: OperationId::from_raw(current),
                },
            )
            .unwrap_or_else(|error| panic!("abort transaction {current}: {error:?}"));
        machine
            .apply(
                owner,
                TransactionLifecycleInput::EndSettled {
                    epoch,
                    outcome: TransactionEndOutcome::Succeeded,
                },
            )
            .unwrap_or_else(|error| panic!("settle transaction {current}: {error:?}"));
    }
    unreachable!("transaction epoch test scalar is positive")
}
