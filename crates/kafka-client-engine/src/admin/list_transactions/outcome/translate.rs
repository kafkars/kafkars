//! Exhaustive core-to-engine translation for Admin `ListTransactions`.

use kafka_client_core::{
    AdminListTransactionsFailureKind as CoreFailureKind,
    AdminListTransactionsTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

use super::{
    AdminListTransactionsBrokerError, AdminListTransactionsDeliveryStatus,
    AdminListTransactionsDiscoveryError, AdminListTransactionsEngineBatch,
    AdminListTransactionsFailure, AdminListTransactionsFailureKind, AdminListTransactionsOutcome,
    AdminListedTransaction,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AdminListTransactionsOutcome {
    match terminal {
        CoreTerminal::Listed(batch) => {
            let (throttle_time_ms, unknown_state_filters, transactions, broker_errors) =
                batch.into_parts();
            AdminListTransactionsOutcome::Listed(AdminListTransactionsEngineBatch {
                throttle_time_ms,
                unknown_state_filters,
                transactions: transactions
                    .into_iter()
                    .map(|transaction| {
                        let (transactional_id, producer_id, transaction_state) =
                            transaction.into_parts();
                        AdminListedTransaction {
                            transactional_id,
                            producer_id,
                            transaction_state,
                        }
                    })
                    .collect(),
                broker_errors: broker_errors
                    .into_iter()
                    .map(|error| {
                        let (broker_id, code) = error.into_parts();
                        AdminListTransactionsBrokerError { broker_id, code }
                    })
                    .collect(),
            })
        }
        CoreTerminal::DiscoveryRejected(error) => {
            let (code, message, message_truncated) = error.into_parts();
            AdminListTransactionsOutcome::DiscoveryRejected(AdminListTransactionsDiscoveryError {
                code,
                message,
                message_truncated,
            })
        }
        CoreTerminal::Failed(failure) => {
            AdminListTransactionsOutcome::Failed(AdminListTransactionsFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> AdminListTransactionsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => AdminListTransactionsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => AdminListTransactionsFailureKind::DriverRejected,
        CoreFailureKind::Transport => AdminListTransactionsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => AdminListTransactionsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => AdminListTransactionsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => AdminListTransactionsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> AdminListTransactionsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => AdminListTransactionsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => AdminListTransactionsDeliveryStatus::PossiblySent,
    }
}
