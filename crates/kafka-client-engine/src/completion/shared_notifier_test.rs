//! Shared typed-port capacity and exact backpressure scenarios.

use std::task::Poll;

use super::{
    CompletionRegistry, CompletionRegistryError, NotificationTicket, PublishTicket, ReclaimStatus,
    SharedNotifier,
    test_support::{GateWake, poll_once},
};

enum TestTicket {
    Small(PublishTicket<u8>),
    Wide(PublishTicket<u16>),
}

impl NotificationTicket for TestTicket {
    fn publish(self) {
        match self {
            Self::Small(ticket) => ticket.publish(),
            Self::Wide(ticket) => ticket.publish(),
        }
    }
}

#[test]
fn bounded_shared_queue_returns_the_exact_typed_terminal_for_retry() {
    let worker = SharedNotifier::start(1, "shared-notifier-backpressure-test")
        .unwrap_or_else(|error| panic!("start shared notifier: {error}"));
    let mut gate_registry =
        CompletionRegistry::with_publisher(1, worker.publish_port(TestTicket::Small));
    let mut queued_registry =
        CompletionRegistry::with_publisher(1, worker.publish_port(TestTicket::Wide));
    let mut retry_registry =
        CompletionRegistry::with_publisher(1, worker.publish_port(TestTicket::Wide));
    let (gate_id, mut gate_observer) = gate_registry
        .reserve()
        .unwrap_or_else(|error| panic!("reserve gate: {error}"));
    let (queued_id, queued_observer) = queued_registry
        .reserve()
        .unwrap_or_else(|error| panic!("reserve queued: {error}"));
    let (retry_id, retry_observer) = retry_registry
        .reserve()
        .unwrap_or_else(|error| panic!("reserve retry: {error}"));
    let gate = GateWake::new();
    assert_eq!(
        poll_once(&mut gate_observer, std::sync::Arc::clone(&gate)),
        Poll::Pending
    );
    assert_eq!(gate_registry.publish(gate_id, 7), Ok(()));
    assert!(gate.wait_until_entered());
    assert_eq!(queued_registry.publish(queued_id, 11), Ok(()));
    let terminal = match retry_registry.publish(retry_id, 13) {
        Err((CompletionRegistryError::NotificationBackpressure, terminal)) => terminal,
        other => panic!("full shared queue must return exact terminal, got {other:?}"),
    };

    gate.release();
    assert_eq!(gate_observer.wait(), Ok(7));
    assert_eq!(queued_observer.wait(), Ok(11));
    let mut terminal = terminal;
    loop {
        match retry_registry.publish(retry_id, terminal) {
            Ok(()) => break,
            Err((CompletionRegistryError::NotificationBackpressure, returned)) => {
                terminal = returned;
                std::thread::yield_now();
            }
            Err((error, returned)) => {
                panic!("retry must retain {returned} through bounded publication: {error}")
            }
        }
    }
    assert_eq!(retry_observer.wait(), Ok(13));
    reclaim(&mut gate_registry, gate_id);
    reclaim(&mut queued_registry, queued_id);
    reclaim(&mut retry_registry, retry_id);
    drop((gate_registry, queued_registry, retry_registry));
    assert_eq!(worker.stop().join_off_notifier(), Ok(()));
}

fn reclaim<T, P>(registry: &mut CompletionRegistry<T, P>, id: super::CompletionId)
where
    T: Send + 'static,
    P: super::registry::CompletionPublisher<T>,
{
    assert_eq!(registry.next_reclaim(), Ok(Some(id)));
    assert_eq!(registry.finish_reclaim(id), Ok(ReclaimStatus::Reclaimed));
}
