//! Ordered batch-result ownership scenarios.

use super::BatchResult;
use crate::{ErrorKind, KafkaError};

#[test]
fn entries_preserve_request_order_and_move_without_reclassification() {
    let rejection = KafkaError::new(ErrorKind::Broker, "broker rejected topic");
    let result = BatchResult::new(vec![
        ("orders".to_owned(), Ok(())),
        ("audit".to_owned(), Err(rejection.clone())),
    ]);

    assert_eq!(result.entries().len(), 2);
    assert_eq!(result.entries()[0].0, "orders");
    assert_eq!(result.entries()[1].0, "audit");
    let outcome: Result<(), KafkaError> = (|| {
        for (_, outcome) in result.into_entries() {
            outcome?;
        }
        Ok(())
    })();
    assert_eq!(outcome, Err(rejection));
}
