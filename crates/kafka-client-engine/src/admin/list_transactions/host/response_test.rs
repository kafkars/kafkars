//! API-key 66 normalized-fact to deterministic-input scenarios.

use kafka_client_core::{AdminListTransactionsBrokerOutcome, AdminListTransactionsInput};

use crate::protocol::admin::list_transactions::{ListTransactionsResponseFacts, ListedTransaction};

use super::response::normalized_input;

#[test]
fn success_preserves_broker_throttle_unknown_states_and_signed_listing() {
    let normalized = ListTransactionsResponseFacts::for_test(
        19,
        None,
        vec!["FutureState".into()],
        vec![ListedTransaction::for_test(
            "orders-writer".into(),
            i64::MIN,
            "Ongoing".into(),
        )],
        91,
    );

    let (input, retained) =
        normalized_input(7, normalized).unwrap_or_else(|()| panic!("core input"));
    let AdminListTransactionsInput::BrokerResponded {
        throttle_time_ms,
        outcome:
            AdminListTransactionsBrokerOutcome::Listed {
                broker_id,
                unknown_state_filters,
                transactions,
            },
    } = input
    else {
        panic!("listed input expected");
    };
    assert_eq!((throttle_time_ms, broker_id), (19, 7));
    assert_eq!(unknown_state_filters, ["FutureState"]);
    assert_eq!(transactions[0].producer_id(), i64::MIN);
    assert!(retained > 0);
}

#[test]
fn nonzero_top_level_error_remains_exact_terminal_data() {
    let normalized =
        ListTransactionsResponseFacts::for_test(3, Some(-32_000), Vec::new(), Vec::new(), 17);

    let (input, _) = normalized_input(11, normalized).unwrap_or_else(|()| panic!("core input"));
    let AdminListTransactionsInput::BrokerResponded {
        outcome: AdminListTransactionsBrokerOutcome::Rejected(error),
        ..
    } = input
    else {
        panic!("broker rejection expected");
    };
    assert_eq!(error.into_parts(), (11, -32_000));
}
