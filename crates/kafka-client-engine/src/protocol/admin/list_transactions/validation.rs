//! Borrowed API-key 66 response checks performed before normalized allocation.

use kafka_wire::ListTransactionsResponse;

use super::ListTransactionsProtocolFailure;

pub(super) const LIST_TRANSACTIONS_MAX_STATE_FILTERS: usize = 4 * 1024;
pub(super) const LIST_TRANSACTIONS_MAX_PRODUCER_FILTERS: usize = 4 * 1024;
pub(super) const LIST_TRANSACTIONS_MAX_TRANSACTIONS: usize = 32 * 1024;
pub(super) const LIST_TRANSACTIONS_MAX_REQUEST_STATE_BYTES: usize = i16::MAX as usize;
pub(super) const LIST_TRANSACTIONS_MAX_STATE_BYTES: usize = 1024;
pub(super) const LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES: usize = i16::MAX as usize;
pub(super) const LIST_TRANSACTIONS_MAX_FILTER_BYTES: usize = 256 * 1024;
pub(super) const LIST_TRANSACTIONS_MAX_PATTERN_BYTES: usize = i16::MAX as usize;
pub(super) const LIST_TRANSACTIONS_MAX_RESPONSE_TEXT_BYTES: usize = 2 * 1024 * 1024;

pub(super) fn validate_response(
    response: &ListTransactionsResponse,
) -> Result<(), ListTransactionsProtocolFailure> {
    if response.error_code != 0 {
        if !response.unknown_state_filters.is_empty() {
            return Err(
                ListTransactionsProtocolFailure::SuccessPayloadWithBrokerError {
                    field: "unknown_state_filters",
                },
            );
        }
        if !response.transaction_states.is_empty() {
            return Err(
                ListTransactionsProtocolFailure::SuccessPayloadWithBrokerError {
                    field: "transaction_states",
                },
            );
        }
        return Ok(());
    }
    if response.unknown_state_filters.len() > LIST_TRANSACTIONS_MAX_STATE_FILTERS {
        return Err(
            ListTransactionsProtocolFailure::TooManyUnknownStateFilters {
                actual: response.unknown_state_filters.len(),
                max: LIST_TRANSACTIONS_MAX_STATE_FILTERS,
            },
        );
    }
    if response.transaction_states.len() > LIST_TRANSACTIONS_MAX_TRANSACTIONS {
        return Err(ListTransactionsProtocolFailure::TooManyTransactions {
            actual: response.transaction_states.len(),
            max: LIST_TRANSACTIONS_MAX_TRANSACTIONS,
        });
    }

    let mut text_bytes = 0usize;
    for state in &response.unknown_state_filters {
        validate_state(state.len())?;
        text_bytes = add_text(text_bytes, state.len())?;
    }
    for transaction in &response.transaction_states {
        if transaction.transactional_id.is_empty() {
            return Err(ListTransactionsProtocolFailure::EmptyTransactionalId);
        }
        if transaction.transactional_id.len() > LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES {
            return Err(ListTransactionsProtocolFailure::TransactionalIdTooLong {
                actual: transaction.transactional_id.len(),
                max: LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES,
            });
        }
        if transaction.transaction_state.is_empty() {
            return Err(ListTransactionsProtocolFailure::EmptyTransactionState);
        }
        validate_state(transaction.transaction_state.len())?;
        text_bytes = add_text(text_bytes, transaction.transactional_id.len())?;
        text_bytes = add_text(text_bytes, transaction.transaction_state.len())?;
    }
    Ok(())
}

fn validate_state(actual: usize) -> Result<(), ListTransactionsProtocolFailure> {
    if actual > LIST_TRANSACTIONS_MAX_STATE_BYTES {
        return Err(ListTransactionsProtocolFailure::StateTooLong {
            actual,
            max: LIST_TRANSACTIONS_MAX_STATE_BYTES,
        });
    }
    Ok(())
}

fn add_text(current: usize, added: usize) -> Result<usize, ListTransactionsProtocolFailure> {
    let required = current.checked_add(added).unwrap_or(usize::MAX);
    if required > LIST_TRANSACTIONS_MAX_RESPONSE_TEXT_BYTES {
        return Err(ListTransactionsProtocolFailure::ResponseTextBytesExceeded {
            required,
            max: LIST_TRANSACTIONS_MAX_RESPONSE_TEXT_BYTES,
        });
    }
    Ok(required)
}
