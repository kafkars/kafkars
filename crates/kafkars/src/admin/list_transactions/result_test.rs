//! Public result accessors retain canonical listing and partial-error facts.

use std::time::Duration;

use super::{ListTransactionsBrokerError, ListTransactionsResult, TransactionListing};

#[test]
fn result_exposes_throttle_listings_unknown_filters_and_errors() {
    let result = ListTransactionsResult::new(
        Duration::from_millis(19),
        vec![TransactionListing::new(
            "alpha".to_owned(),
            -1,
            "Ongoing".to_owned(),
        )],
        vec!["FutureState".to_owned()],
        vec![ListTransactionsBrokerError::new(9, -17)],
    );
    assert_eq!(result.throttle_time(), Duration::from_millis(19));
    assert_eq!(result.transactions()[0].transactional_id(), "alpha");
    assert_eq!(result.unknown_state_filters(), ["FutureState"]);
    assert_eq!(result.broker_errors()[0].broker_id(), 9);

    let (throttle, listings, unknown, errors) = result.into_parts();
    assert_eq!(throttle, Duration::from_millis(19));
    assert_eq!(listings.len(), 1);
    assert_eq!(unknown, ["FutureState"]);
    assert_eq!(errors.len(), 1);
}
