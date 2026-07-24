//! Shared admin completion capacity and typed publication scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Wake, Waker},
    thread::ThreadId,
};

use kafka_client_core::{
    ClusterDescription, CreatePartitionsTerminal, CreateTopicsTerminal, DeleteTopicsTerminal,
    DescribeClusterTerminal,
};

use crate::completion::{CompletionRegistry, ReclaimStatus};

use super::{
    CREATE_PARTITIONS_CAPACITY, CREATE_TOPICS_CAPACITY, DELETE_TOPICS_CAPACITY,
    DESCRIBE_CLUSTER_CAPACITY, completion::AdminCompletionNotifier, test_support::completion_owner,
};

#[test]
fn one_worker_publishes_every_concrete_admin_terminal_off_reactor() {
    let reactor = std::thread::current().id();
    let (mut notifier, ports) = completion_owner();
    let worker = notifier
        .thread_id()
        .unwrap_or_else(|| panic!("shared admin notifier must own one worker"));
    assert_ne!(worker, reactor);

    let mut create = CompletionRegistry::with_publisher(1, ports.create_topics);
    let mut delete = CompletionRegistry::with_publisher(1, ports.delete_topics);
    let mut describe = CompletionRegistry::with_publisher(1, ports.describe_cluster);
    let mut partitions = CompletionRegistry::with_publisher(1, ports.create_partitions);
    let (create_id, mut create_observer) = reserve(&mut create);
    let (delete_id, mut delete_observer) = reserve(&mut delete);
    let (describe_id, mut describe_observer) = reserve(&mut describe);
    let (partitions_id, mut partitions_observer) = reserve(&mut partitions);
    let create_wake = WakeProbe::new();
    let delete_wake = WakeProbe::new();
    let describe_wake = WakeProbe::new();
    let partitions_wake = WakeProbe::new();
    assert_pending(&mut create_observer, Arc::clone(&create_wake));
    assert_pending(&mut delete_observer, Arc::clone(&delete_wake));
    assert_pending(&mut describe_observer, Arc::clone(&describe_wake));
    assert_pending(&mut partitions_observer, Arc::clone(&partitions_wake));

    assert_eq!(
        create.publish(create_id, CreateTopicsTerminal::Topics(Vec::new())),
        Ok(())
    );
    assert_eq!(
        delete.publish(delete_id, DeleteTopicsTerminal::Topics(Vec::new())),
        Ok(())
    );
    assert_eq!(
        describe.publish(
            describe_id,
            DescribeClusterTerminal::Cluster(ClusterDescription::new(
                String::from("cluster"),
                None,
                Vec::new(),
            )),
        ),
        Ok(())
    );
    assert_eq!(
        partitions.publish(partitions_id, CreatePartitionsTerminal::Topics(Vec::new())),
        Ok(())
    );
    for wake in [&create_wake, &delete_wake, &describe_wake, &partitions_wake] {
        assert_eq!(wake.wait(), worker);
    }
    let _create = create_observer
        .wait()
        .unwrap_or_else(|error| panic!("observe CreateTopics: {error}"));
    let _delete = delete_observer
        .wait()
        .unwrap_or_else(|error| panic!("observe DeleteTopics: {error}"));
    let _describe = describe_observer
        .wait()
        .unwrap_or_else(|error| panic!("observe DescribeCluster: {error}"));
    let _partitions = partitions_observer
        .wait()
        .unwrap_or_else(|error| panic!("observe CreatePartitions: {error}"));
    reclaim(&mut create, create_id);
    reclaim(&mut delete, delete_id);
    reclaim(&mut describe, describe_id);
    reclaim(&mut partitions, partitions_id);

    drop((create, delete, describe, partitions));
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop shared notifier: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}

#[test]
fn shared_capacity_is_the_sum_of_the_closed_admin_ticket_set() {
    assert_eq!(
        AdminCompletionNotifier::capacity_for_test(),
        CREATE_TOPICS_CAPACITY
            + DELETE_TOPICS_CAPACITY
            + DESCRIBE_CLUSTER_CAPACITY
            + CREATE_PARTITIONS_CAPACITY
    );
}

#[test]
fn create_partitions_is_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        AdminCompletionNotifier::capacity_for_test().checked_sub(
            CREATE_TOPICS_CAPACITY + DELETE_TOPICS_CAPACITY + DESCRIBE_CLUSTER_CAPACITY
        ),
        Some(CREATE_PARTITIONS_CAPACITY)
    );
}

fn reserve<T, P>(
    registry: &mut CompletionRegistry<T, P>,
) -> (
    crate::completion::CompletionId,
    crate::completion::CompletionObserver<T>,
)
where
    T: Send + 'static,
    P: crate::completion::CompletionPublisher<T>,
{
    registry
        .reserve()
        .unwrap_or_else(|error| panic!("reserve typed admin terminal: {error}"))
}

fn assert_pending<T>(
    observer: &mut crate::completion::CompletionObserver<T>,
    wake: Arc<WakeProbe>,
) {
    let waker = Waker::from(wake);
    assert!(matches!(
        Pin::new(observer).poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));
}

fn reclaim<T, P>(registry: &mut CompletionRegistry<T, P>, id: crate::completion::CompletionId)
where
    T: Send + 'static,
    P: crate::completion::CompletionPublisher<T>,
{
    assert_eq!(registry.next_reclaim(), Ok(Some(id)));
    assert_eq!(registry.finish_reclaim(id), Ok(ReclaimStatus::Reclaimed));
}

struct WakeProbe {
    thread: Mutex<Option<ThreadId>>,
    changed: Condvar,
}

impl WakeProbe {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            thread: Mutex::new(None),
            changed: Condvar::new(),
        })
    }

    fn wait(&self) -> ThreadId {
        let guard = self
            .thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let guard = self
            .changed
            .wait_while(guard, |thread| thread.is_none())
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.unwrap_or_else(|| panic!("notifier wake must record its worker"))
    }
}

impl Wake for WakeProbe {
    fn wake(self: Arc<Self>) {
        *self
            .thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(std::thread::current().id());
        self.changed.notify_all();
    }
}
