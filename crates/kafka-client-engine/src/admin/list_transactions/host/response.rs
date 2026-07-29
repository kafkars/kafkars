//! Exhaustive discovery, API-key 66, and driver-terminal translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminListTransactionsBrokerError, AdminListTransactionsBrokerOutcome,
    AdminListTransactionsInput, AdminListedTransaction as CoreListedTransaction, DeliveryStatus,
};

use crate::{
    driver::{
        ListTransactionsDriverFailureKind, ListTransactionsRawTerminal,
        ListTransactionsRawTerminalFact,
    },
    protocol::admin::{
        list_consumer_groups::{
            ListConsumerGroupsProtocolFailure, NormalizedListConsumerGroupsDiscovery,
            normalize_list_consumer_groups_discovery,
        },
        list_transactions::{
            ListTransactionsProtocolFailure, ListTransactionsResponseFacts,
            normalize_list_transactions_response,
        },
    },
};

pub(super) fn terminal_input(
    raw: &ListTransactionsRawTerminal,
) -> (AdminListTransactionsInput, usize) {
    let retained_bytes = raw.retained_limit();
    match raw.fact() {
        ListTransactionsRawTerminalFact::DiscoveryResponse {
            selected_version,
            response,
        } => discovery_input(normalize_list_consumer_groups_discovery(
            selected_version,
            response,
            retained_bytes,
        )),
        ListTransactionsRawTerminalFact::BrokerResponse {
            broker_id,
            selected_version,
            response,
        } => match normalize_list_transactions_response(selected_version, response, retained_bytes)
        {
            Ok(normalized) => normalized_input(broker_id, normalized)
                .unwrap_or((AdminListTransactionsInput::ResponseTooLarge, 0)),
            Err(error) => (protocol_failure(error), 0),
        },
        ListTransactionsRawTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

fn discovery_input(
    normalized: Result<NormalizedListConsumerGroupsDiscovery, ListConsumerGroupsProtocolFailure>,
) -> (AdminListTransactionsInput, usize) {
    match normalized {
        Ok(NormalizedListConsumerGroupsDiscovery::Brokers {
            broker_ids,
            retained_bytes,
        }) => (
            AdminListTransactionsInput::BrokersDiscovered { broker_ids },
            retained_bytes,
        ),
        Ok(NormalizedListConsumerGroupsDiscovery::Rejected {
            error,
            retained_bytes,
        }) => (
            AdminListTransactionsInput::DiscoveryRejected { error },
            retained_bytes,
        ),
        Err(ListConsumerGroupsProtocolFailure::Compatibility) => (
            AdminListTransactionsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        Err(ListConsumerGroupsProtocolFailure::ResponseTooLarge) => {
            (AdminListTransactionsInput::ResponseTooLarge, 0)
        }
        Err(ListConsumerGroupsProtocolFailure::InvalidResponse) => {
            (AdminListTransactionsInput::InvalidResponse, 0)
        }
    }
}

pub(super) fn normalized_input(
    broker_id: i32,
    normalized: ListTransactionsResponseFacts,
) -> Result<(AdminListTransactionsInput, usize), ()> {
    if broker_id < 0 {
        return Err(());
    }
    let (throttle_time_ms, broker_error_code, unknown_state_filters, transactions, retained_bytes) =
        normalized.into_parts();
    let outcome = if let Some(code) = broker_error_code {
        let code = NonZeroI16::new(code).ok_or(())?;
        AdminListTransactionsBrokerOutcome::Rejected(AdminListTransactionsBrokerError::new(
            broker_id, code,
        ))
    } else {
        let mut listed = Vec::new();
        listed
            .try_reserve_exact(transactions.len())
            .map_err(|_| ())?;
        for transaction in transactions {
            let (transactional_id, producer_id, transaction_state) = transaction.into_parts();
            listed.push(CoreListedTransaction::new(
                transactional_id,
                producer_id,
                transaction_state,
            ));
        }
        AdminListTransactionsBrokerOutcome::Listed {
            broker_id,
            unknown_state_filters,
            transactions: listed,
        }
    };
    Ok((
        AdminListTransactionsInput::BrokerResponded {
            throttle_time_ms,
            outcome,
        },
        retained_bytes,
    ))
}

const fn protocol_failure(error: ListTransactionsProtocolFailure) -> AdminListTransactionsInput {
    match error {
        ListTransactionsProtocolFailure::MissingSelectedVersion
        | ListTransactionsProtocolFailure::UnsupportedApiVersion { .. } => {
            AdminListTransactionsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        ListTransactionsProtocolFailure::RetainedBytes { .. }
        | ListTransactionsProtocolFailure::Allocation { .. } => {
            AdminListTransactionsInput::ResponseTooLarge
        }
        ListTransactionsProtocolFailure::NegativeThrottleTime { .. }
        | ListTransactionsProtocolFailure::SuccessPayloadWithBrokerError { .. }
        | ListTransactionsProtocolFailure::TooManyUnknownStateFilters { .. }
        | ListTransactionsProtocolFailure::TooManyTransactions { .. }
        | ListTransactionsProtocolFailure::EmptyTransactionalId
        | ListTransactionsProtocolFailure::TransactionalIdTooLong { .. }
        | ListTransactionsProtocolFailure::EmptyTransactionState
        | ListTransactionsProtocolFailure::StateTooLong { .. }
        | ListTransactionsProtocolFailure::ResponseTextBytesExceeded { .. }
        | ListTransactionsProtocolFailure::DuplicateUnknownStateFilter
        | ListTransactionsProtocolFailure::DuplicateTransactionalId => {
            AdminListTransactionsInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: ListTransactionsDriverFailureKind,
    delivery: DeliveryStatus,
) -> AdminListTransactionsInput {
    match kind {
        ListTransactionsDriverFailureKind::DeadlineElapsed => {
            AdminListTransactionsInput::DriverDeadlineElapsed { delivery }
        }
        ListTransactionsDriverFailureKind::Compatibility => {
            AdminListTransactionsInput::ProtocolIncompatible { delivery }
        }
        ListTransactionsDriverFailureKind::InvalidResponse => {
            AdminListTransactionsInput::InvalidResponse
        }
        ListTransactionsDriverFailureKind::Transport => {
            AdminListTransactionsInput::TransportFailed { delivery }
        }
    }
}
