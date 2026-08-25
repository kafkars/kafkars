//! Exhaustive stable translation of explicit transaction-end outcomes.

use kafka_client_engine::{
    TransactionEndDeliveryStatus, TransactionEndFailure, TransactionEndFailureKind,
    TransactionEndIntent as EngineEndIntent, TransactionEndObserverError, TransactionEndOutcome,
};

use crate::{DeliveryStatus, ErrorKind, KafkaError, TransactionEndIntent};

pub(in crate::bridge::transaction) fn translate_end_observation(
    intent: TransactionEndIntent,
    result: Result<TransactionEndOutcome, TransactionEndObserverError>,
) -> Result<(), KafkaError> {
    match result {
        Ok(TransactionEndOutcome::Failed(failure)) => Err(translate_end_failure(intent, failure)),
        Ok(TransactionEndOutcome::Committed) if intent == TransactionEndIntent::Commit => Ok(()),
        Ok(TransactionEndOutcome::Aborted) if intent == TransactionEndIntent::Abort => Ok(()),
        Ok(TransactionEndOutcome::Committed | TransactionEndOutcome::Aborted) => {
            Err(KafkaError::new(
                ErrorKind::Internal,
                "transaction end disposition mismatched",
            )
            .with_transaction_end_intent(intent)
            .with_fatal_disposition())
        }
        Err(TransactionEndObserverError::AlreadyObserved) => Err(KafkaError::new(
            ErrorKind::State,
            "transaction end was already observed",
        )
        .with_transaction_end_intent(intent)),
        Err(TransactionEndObserverError::Stale) => Err(KafkaError::new(
            ErrorKind::Internal,
            "transaction end observer became stale",
        )
        .with_transaction_end_intent(intent)),
    }
}

fn translate_end_failure(
    expected_intent: TransactionEndIntent,
    failure: TransactionEndFailure,
) -> KafkaError {
    translate_end_failure_parts(
        expected_intent,
        failure.intent(),
        failure.kind(),
        failure.delivery(),
        failure.broker_code(),
    )
}

pub(in crate::bridge::transaction) fn translate_end_failure_parts(
    expected_intent: TransactionEndIntent,
    engine_intent: EngineEndIntent,
    failure_kind: TransactionEndFailureKind,
    delivery: TransactionEndDeliveryStatus,
    broker_code: Option<i16>,
) -> KafkaError {
    let actual_intent = translate_end_intent(engine_intent);
    if actual_intent != expected_intent {
        return KafkaError::new(
            ErrorKind::Internal,
            "transaction end failure intent did not match its observer",
        )
        .with_transaction_end_intent(expected_intent)
        .with_fatal_disposition();
    }
    let kind = end_failure_kind(failure_kind, broker_code);
    KafkaError::new(kind, format!("transaction end failed: {failure_kind:?}"))
        .with_delivery_status(translate_end_delivery(delivery))
        .with_broker_code(broker_code)
        .with_transaction_end_intent(expected_intent)
        .with_fatal_disposition()
}

const fn end_failure_kind(kind: TransactionEndFailureKind, broker_code: Option<i16>) -> ErrorKind {
    match kind {
        TransactionEndFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        TransactionEndFailureKind::DriverRejected => ErrorKind::Backpressure,
        TransactionEndFailureKind::Transport => ErrorKind::Transport,
        TransactionEndFailureKind::Compatibility => ErrorKind::Compatibility,
        TransactionEndFailureKind::Fenced if matches!(broker_code, Some(47 | 90)) => {
            ErrorKind::Fenced
        }
        TransactionEndFailureKind::InvalidResponse
        | TransactionEndFailureKind::Fenced
        | TransactionEndFailureKind::Broker => ErrorKind::Broker,
        TransactionEndFailureKind::DriverClosed
        | TransactionEndFailureKind::Correlation
        | TransactionEndFailureKind::Lifecycle => ErrorKind::Internal,
        TransactionEndFailureKind::Access => ErrorKind::Access,
        TransactionEndFailureKind::Coordinator => ErrorKind::Routing,
    }
}

const fn translate_end_delivery(delivery: TransactionEndDeliveryStatus) -> DeliveryStatus {
    match delivery {
        TransactionEndDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        TransactionEndDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

const fn translate_end_intent(intent: EngineEndIntent) -> TransactionEndIntent {
    match intent {
        EngineEndIntent::Commit => TransactionEndIntent::Commit,
        EngineEndIntent::Abort => TransactionEndIntent::Abort,
    }
}
