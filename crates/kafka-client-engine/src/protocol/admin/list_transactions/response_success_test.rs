//! Exact error and successful canonical fact scenarios for flexible API key 66.

use kafka_wire::{ListTransactionsResponse, list_transactions_response::TransactionState};

use super::{ListTransactionsResponseFacts, normalize_list_transactions_response};

pub(super) const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn success_preserves_unknown_states_signed_ids_and_canonical_order() {
    let response = response(
        21,
        0,
        &["SomeFutureState", "AnotherFutureState"],
        vec![
            transaction("zeta", i64::MIN, "FutureState"),
            transaction("alpha", -1, "Ongoing"),
        ],
    );
    for version in 0..=2 {
        let facts = normalize(Some(version), &response, LIMIT)
            .unwrap_or_else(|error| panic!("v{version} response: {error:?}"));
        assert_eq!(facts.throttle_time_ms(), 21);
        assert_eq!(facts.broker_error_code(), None);
        assert_eq!(
            facts.unknown_state_filters(),
            ["AnotherFutureState", "SomeFutureState"]
        );
        assert_eq!(facts.transactions()[0].transactional_id(), "alpha");
        assert_eq!(facts.transactions()[0].producer_id(), -1);
        assert_eq!(facts.transactions()[0].transaction_state(), "Ongoing");
        assert_eq!(facts.transactions()[1].transactional_id(), "zeta");
        assert_eq!(facts.transactions()[1].producer_id(), i64::MIN);
        assert_eq!(facts.transactions()[1].transaction_state(), "FutureState");
        assert!(facts.retained_bytes() > 0);
    }
}

#[test]
fn top_level_error_preserves_exact_signed_code_without_payload() {
    let facts = normalize(Some(2), &response(7, -32_000, &[], Vec::new()), LIMIT)
        .unwrap_or_else(|error| panic!("broker error: {error:?}"));
    let (throttle, code, unknown, transactions, retained) = facts.into_parts();
    assert_eq!(throttle, 7);
    assert_eq!(code, Some(-32_000));
    assert!(unknown.is_empty());
    assert!(transactions.is_empty());
    assert!(retained > 0);
}

pub(super) fn normalize(
    version: Option<i16>,
    response: &ListTransactionsResponse,
    limit: usize,
) -> Result<ListTransactionsResponseFacts, super::ListTransactionsProtocolFailure> {
    normalize_list_transactions_response(version, response, limit)
}

pub(super) fn response(
    throttle_time_ms: i32,
    error_code: i16,
    unknown_states: &[&str],
    transactions: Vec<TransactionState>,
) -> ListTransactionsResponse {
    let mut response = ListTransactionsResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.error_code = error_code;
    response.unknown_state_filters = unknown_states.iter().copied().map(Into::into).collect();
    response.transaction_states = transactions;
    response
}

pub(super) fn transaction(
    transactional_id: &str,
    producer_id: i64,
    state: &str,
) -> TransactionState {
    let mut transaction = TransactionState::default();
    transaction.transactional_id = transactional_id.into();
    transaction.producer_id = producer_id;
    transaction.transaction_state = state.into();
    transaction
}
