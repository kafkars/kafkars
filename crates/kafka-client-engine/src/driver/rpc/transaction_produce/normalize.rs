//! Transaction-health classification over existing Produce normalization.

use std::sync::Arc;

use kafka_client_core::{
    DeliveryStatus, ProducerBrokerFailure, ProducerBrokerFailureKind, TransactionEpoch,
    TransactionSendAttempt, TransactionSendId,
};
use kafka_driver::RequestError;
use kafka_wire::ProduceResponse;

use crate::{
    driver::{request_failure_delivery, request_failure_kind},
    protocol::produce_response::{ProduceResponseFailure, normalize_explicit_produce_response},
};

use super::model::{
    RouteEvidence, TransactionProduceFailure, TransactionProduceFailureKind,
    TransactionProduceTerminal, TransactionProduceTerminalFact,
};

pub(super) enum TransactionProduceResult {
    Response(ProduceResponse),
    Driver(RequestError),
    CompletionLost,
    DriverShutdown,
}

#[cfg_attr(
    not(test),
    expect(
        clippy::needless_pass_by_value,
        reason = "terminal normalization consumes the exact topic owner retained for correlation"
    )
)]
pub(super) fn normalize_terminal(
    epoch: TransactionEpoch,
    send_id: TransactionSendId,
    topic: Arc<str>,
    partition: i32,
    attempt: TransactionSendAttempt,
    result: TransactionProduceResult,
    evidence: RouteEvidence,
) -> TransactionProduceTerminal {
    let fact = match result {
        TransactionProduceResult::Response(response) => {
            normalize_response(epoch, send_id, topic.as_ref(), partition, &response)
        }
        TransactionProduceResult::Driver(error) => {
            let failure = TransactionProduceFailure::new(
                TransactionProduceFailureKind::Driver(request_failure_kind(&error)),
                request_failure_delivery(&error),
            );
            failure_fact(
                epoch,
                send_id,
                failure,
                failure.delivery() == DeliveryStatus::PossiblySent,
            )
        }
        TransactionProduceResult::CompletionLost => fatal(
            epoch,
            send_id,
            TransactionProduceFailure::new(
                TransactionProduceFailureKind::CompletionLost,
                DeliveryStatus::PossiblySent,
            ),
        ),
        TransactionProduceResult::DriverShutdown => fatal(
            epoch,
            send_id,
            TransactionProduceFailure::new(
                TransactionProduceFailureKind::DriverShutdown,
                DeliveryStatus::PossiblySent,
            ),
        ),
    };
    TransactionProduceTerminal::new(
        #[cfg(test)]
        topic,
        #[cfg(test)]
        partition,
        attempt,
        fact,
        evidence,
    )
}

fn normalize_response(
    epoch: TransactionEpoch,
    send_id: TransactionSendId,
    topic: &str,
    partition: i32,
    response: &ProduceResponse,
) -> TransactionProduceTerminalFact {
    match normalize_explicit_produce_response(response, topic, partition) {
        Ok(success) => TransactionProduceTerminalFact::Succeeded {
            epoch,
            send_id,
            success,
        },
        Err(ProduceResponseFailure::Broker { failure, delivery }) => failure_fact(
            epoch,
            send_id,
            TransactionProduceFailure::new(
                TransactionProduceFailureKind::Broker(failure),
                delivery,
            ),
            broker_is_fatal(failure),
        ),
        Err(ProduceResponseFailure::Protocol { failure, delivery }) => fatal(
            epoch,
            send_id,
            TransactionProduceFailure::new(
                TransactionProduceFailureKind::Protocol(failure),
                delivery,
            ),
        ),
    }
}

const fn broker_is_fatal(failure: ProducerBrokerFailure) -> bool {
    match failure.kind() {
        ProducerBrokerFailureKind::ProducerIdentity
        | ProducerBrokerFailureKind::ProducerFenced
        | ProducerBrokerFailureKind::Unknown => true,
        ProducerBrokerFailureKind::Retriable => {
            matches!(failure.code(), 7 | 13 | 20 | 56)
        }
        ProducerBrokerFailureKind::Routing
        | ProducerBrokerFailureKind::AccessRejected
        | ProducerBrokerFailureKind::InvalidRecord
        | ProducerBrokerFailureKind::Compatibility => false,
    }
}

const fn failure_fact(
    epoch: TransactionEpoch,
    send_id: TransactionSendId,
    failure: TransactionProduceFailure,
    fatal_failure: bool,
) -> TransactionProduceTerminalFact {
    if fatal_failure {
        fatal(epoch, send_id, failure)
    } else {
        TransactionProduceTerminalFact::AbortRequired {
            epoch,
            send_id,
            failure,
        }
    }
}

const fn fatal(
    epoch: TransactionEpoch,
    send_id: TransactionSendId,
    failure: TransactionProduceFailure,
) -> TransactionProduceTerminalFact {
    TransactionProduceTerminalFact::Fatal {
        epoch,
        send_id,
        failure,
    }
}
