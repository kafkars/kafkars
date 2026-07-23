//! Diagnostics for engine-owned producer observation lifecycle failures.

use std::error::Error as _;

use crate::{ProducerDeliveryError, ProducerObserverError};

#[test]
fn observer_errors_remain_explicit_engine_owned_diagnostics() {
    let stale = ProducerObserverError::Stale;
    assert_eq!(stale.to_string(), "producer delivery observer is stale");

    let error = ProducerDeliveryError::Observer(stale);
    assert_eq!(error.to_string(), "producer delivery observer is stale");
    assert!(error.source().is_some());
}
