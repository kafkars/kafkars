//! Exhaustive stable translation of engine Admin `ListTransactions` outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{ListTransactionsBrokerError, ListTransactionsResult, TransactionListing},
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, DeliveryStatus, DiscoveryError,
        Failure, FailureKind, ObserverError, Outcome,
    },
    operation::AdminListTransactionsResult,
};

pub(super) fn translate_admission_error(error: AdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: AdmissionErrorKind) -> KafkaError {
    let public = match kind {
        AdmissionErrorKind::InvalidRequest | AdmissionErrorKind::InvalidDeadline => {
            ErrorKind::Configuration
        }
        AdmissionErrorKind::Contended
        | AdmissionErrorKind::Capacity
        | AdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        AdmissionErrorKind::Closed => ErrorKind::State,
        AdmissionErrorKind::IdentityExhausted | AdmissionErrorKind::HostUnavailable => {
            ErrorKind::Internal
        }
    };
    KafkaError::new(
        public,
        format!("ListTransactions admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "ListTransactions was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "ListTransactions was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminListTransactionsResult {
    match result {
        Ok(Outcome::Listed(batch)) => {
            let (throttle_time_ms, unknown, transactions, errors) = batch.into_parts();
            let transactions = transactions
                .into_iter()
                .map(kafka_client_engine::AdminListedTransaction::into_parts)
                .collect::<Vec<_>>();
            let errors = errors
                .into_iter()
                .map(kafka_client_engine::AdminListTransactionsBrokerError::into_parts)
                .collect::<Vec<_>>();
            Ok(translate_listed_parts(
                throttle_time_ms,
                unknown,
                transactions,
                errors,
            ))
        }
        Ok(Outcome::DiscoveryRejected(error)) => Err(translate_discovery_error(error)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_discovery_error(error: DiscoveryError) -> KafkaError {
    let (code, message, truncated) = error.into_parts();
    translate_discovery_parts(code, message.as_deref(), truncated)
}

pub(super) fn translate_discovery_parts(
    code: i16,
    message: Option<&str>,
    truncated: bool,
) -> KafkaError {
    let detail = message.map_or_else(
        || format!("Kafka rejected ListTransactions broker discovery with code {code}"),
        |message| {
            format!("Kafka rejected ListTransactions broker discovery with code {code}: {message}")
        },
    );
    KafkaError::new(ErrorKind::Broker, detail)
        .with_broker_code(Some(code))
        .with_delivery_status(PublicDeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(truncated)
}

pub(super) fn translate_listed_parts(
    throttle_time_ms: u32,
    mut unknown: Vec<String>,
    transactions: Vec<(String, i64, String)>,
    errors: Vec<(i32, i16)>,
) -> ListTransactionsResult {
    unknown.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    unknown.dedup();
    let mut transactions = transactions
        .into_iter()
        .map(|(transactional_id, producer_id, transaction_state)| {
            TransactionListing::new(transactional_id, producer_id, transaction_state)
        })
        .collect::<Vec<_>>();
    transactions.sort_unstable_by(|left, right| {
        left.transactional_id()
            .as_bytes()
            .cmp(right.transactional_id().as_bytes())
    });
    let mut errors = errors
        .into_iter()
        .map(|(broker_id, code)| ListTransactionsBrokerError::new(broker_id, code))
        .collect::<Vec<_>>();
    errors.sort_unstable_by_key(|error| error.broker_id());
    ListTransactionsResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        transactions,
        unknown,
        errors,
    )
}

fn translate_failure(failure: Failure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(kind: FailureKind, delivery: DeliveryStatus) -> KafkaError {
    let public = match kind {
        FailureKind::DeadlineElapsed => ErrorKind::Timeout,
        FailureKind::DriverRejected | FailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        FailureKind::Transport => ErrorKind::Transport,
        FailureKind::Compatibility => ErrorKind::Compatibility,
        FailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("ListTransactions failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

const fn translate_delivery(delivery: DeliveryStatus) -> PublicDeliveryStatus {
    match delivery {
        DeliveryStatus::NotSent => PublicDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => PublicDeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(error: ObserverError) -> KafkaError {
    let public = match error {
        ObserverError::AlreadyObserved => ErrorKind::State,
        ObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
