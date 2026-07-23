//! Producer flush error formatting and source-chain scenarios.

use crate::{ProducerFlushError, ProducerObserverError};

#[test]
fn execution_loss_and_observer_failures_remain_distinct() {
    let unavailable = ProducerFlushError::ExecutionUnavailable;
    let observer = ProducerFlushError::Observer(ProducerObserverError::Stale);

    assert!(unavailable.to_string().contains("execution"));
    assert!(observer.to_string().contains("stale"));
}
