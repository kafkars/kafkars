//! Public immediate-admission error trait scenarios.

use std::error::Error;

use super::TrySendError;
use crate::{ErrorKind, KafkaError, Record};

#[test]
fn try_send_error_displays_and_sources_the_semantic_failure() {
    let error = KafkaError::new(ErrorKind::Backpressure, "producer capacity is exhausted");
    let rejection = TrySendError::new(Record::to("orders"), error);

    assert_eq!(rejection.to_string(), "producer capacity is exhausted");
    assert_eq!(
        rejection.source().map(ToString::to_string).as_deref(),
        Some("producer capacity is exhausted")
    );
}
