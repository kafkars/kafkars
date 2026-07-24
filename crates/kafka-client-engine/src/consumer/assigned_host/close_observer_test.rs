//! Runtime-neutral close observation over the shared completion cell.

use kafka_client_core::{AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine};

use crate::completion::CompletionRegistry;

use super::{
    close_observer::{AssignedConsumerCloseObserver, AssignedConsumerCloseTerminal},
    completion::AssignedConsumerCompletionNotifier,
};

#[test]
fn blocking_wait_observes_the_same_notifier_terminal() {
    let (mut notifier, publisher) = AssignedConsumerCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start notifier: {error}"));
    let mut completions = CompletionRegistry::with_publisher(1, publisher);
    let (completion_id, observer) = completions
        .reserve()
        .unwrap_or_else(|error| panic!("reserve close: {error}"));
    let close_id = close_id();
    completions
        .publish(
            completion_id,
            AssignedConsumerCloseTerminal::Closed(close_id),
        )
        .unwrap_or_else(|(error, _terminal)| panic!("publish close: {error}"));

    let terminal = AssignedConsumerCloseObserver::from_completion(observer)
        .wait()
        .unwrap_or_else(|error| panic!("observe close: {error:?}"));

    assert_eq!(terminal, AssignedConsumerCloseTerminal::Closed(close_id));
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join notifier: {error}"));
}

fn close_id() -> kafka_client_core::AssignedConsumerCloseId {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("accept close: {error}"));
    let AssignedConsumerEffect::AcceptClose { close_id } = transition.effects()[0] else {
        panic!("first close effect must accept");
    };
    close_id
}

#[test]
fn observer_drop_abandons_without_revoking_terminal_authority() {
    let (mut notifier, publisher) = AssignedConsumerCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start notifier: {error}"));
    let mut completions = CompletionRegistry::with_publisher(1, publisher);
    let (completion_id, observer) = completions
        .reserve()
        .unwrap_or_else(|error| panic!("reserve close: {error}"));
    drop(AssignedConsumerCloseObserver::from_completion(observer));

    completions
        .publish(
            completion_id,
            AssignedConsumerCloseTerminal::ExecutionUnavailable,
        )
        .unwrap_or_else(|(error, _terminal)| panic!("publish abandoned close: {error}"));

    assert_eq!(completions.unsettled_len(), 0);
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join notifier: {error}"));
}
