//! Compatibility, malformed payload, duplicate, hostile count, and capacity scenarios.

use super::{
    ListTransactionsProtocolFailure,
    response_success_test::{LIMIT, normalize, response, transaction},
    validation::{
        LIST_TRANSACTIONS_MAX_STATE_BYTES, LIST_TRANSACTIONS_MAX_STATE_FILTERS,
        LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES, LIST_TRANSACTIONS_MAX_TRANSACTIONS,
    },
};

#[test]
fn selected_version_and_throttle_are_strict() {
    let valid = response(0, 0, &[], Vec::new());
    assert_eq!(
        normalize(None, &valid, LIMIT),
        Err(ListTransactionsProtocolFailure::MissingSelectedVersion)
    );
    for actual in [-1, 3, i16::MAX] {
        assert_eq!(
            normalize(Some(actual), &valid, LIMIT),
            Err(ListTransactionsProtocolFailure::UnsupportedApiVersion { actual })
        );
    }
    assert_eq!(
        normalize(Some(2), &response(-1, 0, &[], Vec::new()), LIMIT),
        Err(ListTransactionsProtocolFailure::NegativeThrottleTime { actual: -1 })
    );
}

#[test]
fn broker_error_rejects_ambiguous_success_fields() {
    assert_eq!(
        normalize(Some(2), &response(0, 1, &["Unknown"], Vec::new()), LIMIT),
        Err(
            ListTransactionsProtocolFailure::SuccessPayloadWithBrokerError {
                field: "unknown_state_filters"
            }
        )
    );
    assert_eq!(
        normalize(
            Some(2),
            &response(0, 1, &[], vec![transaction("tx", -1, "Ongoing")]),
            LIMIT,
        ),
        Err(
            ListTransactionsProtocolFailure::SuccessPayloadWithBrokerError {
                field: "transaction_states"
            }
        )
    );
}

#[test]
fn transaction_identity_and_state_shapes_are_bounded_without_whitelisting() {
    assert_eq!(
        normalize(
            Some(2),
            &response(0, 0, &[], vec![transaction("", -1, "Ongoing")]),
            LIMIT,
        ),
        Err(ListTransactionsProtocolFailure::EmptyTransactionalId)
    );
    let long_id = "x".repeat(LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES + 1);
    assert!(matches!(
        normalize(
            Some(2),
            &response(0, 0, &[], vec![transaction(&long_id, -1, "Ongoing")]),
            LIMIT,
        ),
        Err(ListTransactionsProtocolFailure::TransactionalIdTooLong { .. })
    ));
    assert_eq!(
        normalize(
            Some(2),
            &response(0, 0, &[], vec![transaction("tx", -1, "")]),
            LIMIT,
        ),
        Err(ListTransactionsProtocolFailure::EmptyTransactionState)
    );
    let long_state = "x".repeat(LIST_TRANSACTIONS_MAX_STATE_BYTES + 1);
    assert!(matches!(
        normalize(
            Some(2),
            &response(0, 0, &[], vec![transaction("tx", -1, &long_state)]),
            LIMIT,
        ),
        Err(ListTransactionsProtocolFailure::StateTooLong { .. })
    ));
}

#[test]
fn duplicate_and_hostile_collections_and_retained_capacity_are_rejected() {
    assert_eq!(
        normalize(
            Some(2),
            &response(0, 0, &["Unknown", "Unknown"], Vec::new()),
            LIMIT,
        ),
        Err(ListTransactionsProtocolFailure::DuplicateUnknownStateFilter)
    );
    assert_eq!(
        normalize(
            Some(2),
            &response(
                0,
                0,
                &[],
                vec![
                    transaction("tx", -1, "Ongoing"),
                    transaction("tx", 9, "Empty"),
                ],
            ),
            LIMIT,
        ),
        Err(ListTransactionsProtocolFailure::DuplicateTransactionalId)
    );

    let unknown = vec![""; LIST_TRANSACTIONS_MAX_STATE_FILTERS + 1];
    assert!(matches!(
        normalize(Some(2), &response(0, 0, &unknown, Vec::new()), LIMIT),
        Err(ListTransactionsProtocolFailure::TooManyUnknownStateFilters { .. })
    ));
    let transactions = (0..=LIST_TRANSACTIONS_MAX_TRANSACTIONS)
        .map(|index| transaction(&format!("tx-{index}"), -1, "Ongoing"))
        .collect();
    assert!(matches!(
        normalize(Some(2), &response(0, 0, &[], transactions), LIMIT),
        Err(ListTransactionsProtocolFailure::TooManyTransactions { .. })
    ));
    assert!(matches!(
        normalize(
            Some(2),
            &response(0, 0, &[], vec![transaction("tx", -1, "Ongoing")]),
            0,
        ),
        Err(ListTransactionsProtocolFailure::RetainedBytes { .. })
    ));
}
