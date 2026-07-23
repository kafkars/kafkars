//! Producer flush observer success, failure, and type-fencing scenarios.

use kafka_client_core::{DeliveryStatus, ProducerCompletion, ProducerFailure};

use crate::{
    ProducerFlushError, ProducerFlushObserver, ProducerObserverError,
    completion::CompletionRegistry, producer::ProducerTerminal,
};

#[test]
fn observer_reports_successful_flush() {
    let (mut registry, id, observer) = reserved();
    assert_eq!(
        registry.publish(id, ProducerTerminal::flush_completed()),
        Ok(())
    );
    assert_eq!(
        ProducerFlushObserver::from_completion(observer).wait(),
        Ok(())
    );
}

#[test]
fn observer_reports_execution_loss() {
    let (mut registry, id, observer) = reserved();
    assert_eq!(
        registry.publish(id, ProducerTerminal::execution_unavailable()),
        Ok(())
    );
    assert_eq!(
        ProducerFlushObserver::from_completion(observer).wait(),
        Err(ProducerFlushError::ExecutionUnavailable)
    );
}

#[test]
fn observer_rejects_a_record_terminal() {
    let (mut registry, id, observer) = reserved();
    let terminal = ProducerTerminal::record(ProducerCompletion::Failed(
        ProducerFailure::execution_unavailable(DeliveryStatus::NotSent),
    ));
    assert_eq!(registry.publish(id, terminal), Ok(()));
    assert_eq!(
        ProducerFlushObserver::from_completion(observer).wait(),
        Err(ProducerFlushError::Observer(
            ProducerObserverError::TerminalTypeMismatch
        ))
    );
}

fn reserved() -> (
    CompletionRegistry<ProducerTerminal>,
    crate::completion::CompletionId,
    crate::completion::CompletionObserver<ProducerTerminal>,
) {
    let mut registry =
        CompletionRegistry::start(1).unwrap_or_else(|error| panic!("notifier failed: {error}"));
    let (id, observer) = registry
        .reserve()
        .unwrap_or_else(|error| panic!("completion reserve failed: {error}"));
    (registry, id, observer)
}
