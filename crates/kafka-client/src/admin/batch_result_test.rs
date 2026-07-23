//! Ordered batch-result ownership scenarios.

use super::BatchResult;
use crate::{ErrorKind, KafkaError};

#[test]
fn entries_preserve_request_order_and_move_without_reclassification() {
    let result = BatchResult::new(vec![
        ("orders".to_owned(), Ok(())),
        (
            "audit".to_owned(),
            Err(KafkaError::new(ErrorKind::Broker, "broker rejected topic")),
        ),
    ]);

    assert_eq!(result.entries()[0].0, "orders");
    assert_eq!(result.entries()[1].0, "audit");
    assert_eq!(result.into_entries().len(), 2);
}
