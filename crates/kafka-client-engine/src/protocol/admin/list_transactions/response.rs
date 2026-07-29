//! Strict compatible-version normalization of API-key 66 responses.

use kafka_wire::ListTransactionsResponse;

use super::{
    ListTransactionsResponseFacts,
    materialize::materialize_success,
    retention::{broker_error_charge, ensure_limit, source_success_charge},
    validation::validate_response,
    version::supports_list_transactions_version,
};

/// Compatibility, hostile shape, scalar, duplicate, allocation, or capacity failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListTransactionsProtocolFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion {
        actual: i16,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    SuccessPayloadWithBrokerError {
        field: &'static str,
    },
    TooManyUnknownStateFilters {
        actual: usize,
        max: usize,
    },
    TooManyTransactions {
        actual: usize,
        max: usize,
    },
    EmptyTransactionalId,
    TransactionalIdTooLong {
        actual: usize,
        max: usize,
    },
    EmptyTransactionState,
    StateTooLong {
        actual: usize,
        max: usize,
    },
    ResponseTextBytesExceeded {
        required: usize,
        max: usize,
    },
    DuplicateUnknownStateFilter,
    DuplicateTransactionalId,
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

/// Validates and copies one flexible v0-v2 response into deterministic facts.
pub(crate) fn normalize_list_transactions_response(
    selected_version: Option<i16>,
    response: &ListTransactionsResponse,
    retained_limit: usize,
) -> Result<ListTransactionsResponseFacts, ListTransactionsProtocolFailure> {
    let selected_version =
        selected_version.ok_or(ListTransactionsProtocolFailure::MissingSelectedVersion)?;
    if !supports_list_transactions_version(selected_version) {
        return Err(ListTransactionsProtocolFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        ListTransactionsProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    validate_response(response)?;
    if response.error_code != 0 {
        let required = broker_error_charge();
        ensure_limit(required, retained_limit)?;
        return Ok(ListTransactionsResponseFacts::new(
            throttle_time_ms,
            Some(response.error_code),
            Vec::new(),
            Vec::new(),
            required,
        ));
    }
    let required = source_success_charge(response).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    materialize_success(throttle_time_ms, response, required, retained_limit)
}
