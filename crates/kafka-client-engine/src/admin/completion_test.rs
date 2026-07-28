//! Shared admin completion capacity and typed publication scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Wake, Waker},
    thread::ThreadId,
};

use kafka_client_core::{
    AdminDescribeLogDirsBatch, AdminDescribeLogDirsTerminal, AdminListOffsetsBatch,
    AdminListOffsetsTerminal, AlterConsumerGroupOffsetsBatch, AlterConsumerGroupOffsetsTerminal,
    AlterPartitionReassignmentsBatch, AlterPartitionReassignmentsTerminal,
    AlterReplicaLogDirsBatch, AlterReplicaLogDirsTerminal, ClusterDescription,
    CreatePartitionsTerminal, CreateTopicsTerminal, DeleteConsumerGroupOffsetsBatch,
    DeleteConsumerGroupOffsetsTerminal, DeleteTopicsTerminal, DescribeClusterTerminal,
    DescribeConfigsBatch, DescribeConfigsTerminal, DescribeTopicsTerminal,
    IncrementalAlterConfigsBatch, IncrementalAlterConfigsTerminal, ListConsumerGroupOffsetsBatch,
    ListConsumerGroupOffsetsTerminal, ListPartitionReassignmentsBatch,
    ListPartitionReassignmentsTerminal,
};

use crate::completion::{CompletionRegistry, ReclaimStatus};

use super::test_support::completion_owner;

#[test]
fn one_worker_publishes_every_concrete_admin_terminal_off_reactor() {
    let reactor = std::thread::current().id();
    let (mut notifier, ports) = completion_owner();
    let worker = notifier
        .thread_id()
        .unwrap_or_else(|| panic!("shared admin notifier must own one worker"));
    assert_ne!(worker, reactor);

    let mut create = PendingTerminal::new(ports.create_topics);
    let mut delete = PendingTerminal::new(ports.delete_topics);
    let mut describe = PendingTerminal::new(ports.describe_cluster);
    let mut partitions = PendingTerminal::new(ports.create_partitions);
    let mut topics = PendingTerminal::new(ports.describe_topics);
    let mut configs = PendingTerminal::new(ports.describe_configs);
    let mut alter_configs = PendingTerminal::new(ports.incremental_alter_configs);
    let mut group_offsets = PendingTerminal::new(ports.list_consumer_group_offsets);
    let mut group_offset_delete = PendingTerminal::new(ports.delete_consumer_group_offsets);
    let mut group_offset_alter = PendingTerminal::new(ports.alter_consumer_group_offsets);
    let mut list_offsets = PendingTerminal::new(ports.admin_list_offsets);
    let mut reassignments = PendingTerminal::new(ports.list_partition_reassignments);
    let mut alterations = PendingTerminal::new(ports.alter_partition_reassignments);
    let mut log_directories = PendingTerminal::new(ports.describe_log_dirs);
    let mut log_dir_alterations = PendingTerminal::new(ports.alter_replica_log_dirs);

    create.publish(CreateTopicsTerminal::Topics(Vec::new()));
    delete.publish(DeleteTopicsTerminal::Topics(Vec::new()));
    describe.publish(DescribeClusterTerminal::Cluster(ClusterDescription::new(
        String::from("cluster"),
        None,
        Vec::new(),
    )));
    partitions.publish(CreatePartitionsTerminal::Topics(Vec::new()));
    topics.publish(DescribeTopicsTerminal::Topics(Vec::new()));
    configs.publish(DescribeConfigsTerminal::Configs(DescribeConfigsBatch::new(
        0,
        Vec::new(),
    )));
    alter_configs.publish(IncrementalAlterConfigsTerminal::Configs(
        IncrementalAlterConfigsBatch::new(0, Vec::new()),
    ));
    group_offsets.publish(ListConsumerGroupOffsetsTerminal::Offsets(
        ListConsumerGroupOffsetsBatch::new(0, Vec::new()),
    ));
    group_offset_delete.publish(DeleteConsumerGroupOffsetsTerminal::Deleted(
        DeleteConsumerGroupOffsetsBatch::new(0, Vec::new()),
    ));
    group_offset_alter.publish(AlterConsumerGroupOffsetsTerminal::Altered(
        AlterConsumerGroupOffsetsBatch::new(0, Vec::new()),
    ));
    list_offsets.publish(AdminListOffsetsTerminal::Listed(
        AdminListOffsetsBatch::new(0, Vec::new()),
    ));
    reassignments.publish(ListPartitionReassignmentsTerminal::Reassignments(
        ListPartitionReassignmentsBatch::new(0, Vec::new()),
    ));
    alterations.publish(AlterPartitionReassignmentsTerminal::Altered(
        AlterPartitionReassignmentsBatch::new(0, Vec::new()),
    ));
    log_directories.publish(AdminDescribeLogDirsTerminal::Described(
        AdminDescribeLogDirsBatch::new(0, Vec::new()),
    ));
    log_dir_alterations.publish(AlterReplicaLogDirsTerminal::Altered(
        AlterReplicaLogDirsBatch::new(0, Vec::new()),
    ));

    create.observe_and_reclaim(worker);
    delete.observe_and_reclaim(worker);
    describe.observe_and_reclaim(worker);
    partitions.observe_and_reclaim(worker);
    topics.observe_and_reclaim(worker);
    configs.observe_and_reclaim(worker);
    alter_configs.observe_and_reclaim(worker);
    group_offsets.observe_and_reclaim(worker);
    group_offset_delete.observe_and_reclaim(worker);
    group_offset_alter.observe_and_reclaim(worker);
    list_offsets.observe_and_reclaim(worker);
    reassignments.observe_and_reclaim(worker);
    alterations.observe_and_reclaim(worker);
    log_directories.observe_and_reclaim(worker);
    log_dir_alterations.observe_and_reclaim(worker);

    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop shared notifier: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}

struct PendingTerminal<T, P>
where
    T: Send + 'static,
    P: crate::completion::CompletionPublisher<T>,
{
    registry: CompletionRegistry<T, P>,
    id: crate::completion::CompletionId,
    observer: crate::completion::CompletionObserver<T>,
    wake: Arc<WakeProbe>,
}

impl<T, P> PendingTerminal<T, P>
where
    T: Send + 'static,
    P: crate::completion::CompletionPublisher<T>,
{
    fn new(publisher: P) -> Self {
        let mut registry = CompletionRegistry::with_publisher(1, publisher);
        let (id, mut observer) = registry
            .reserve()
            .unwrap_or_else(|error| panic!("reserve typed admin terminal: {error}"));
        let wake = WakeProbe::new();
        let waker = Waker::from(Arc::clone(&wake));
        assert!(matches!(
            Pin::new(&mut observer).poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        Self {
            registry,
            id,
            observer,
            wake,
        }
    }

    fn publish(&mut self, terminal: T) {
        assert!(matches!(self.registry.publish(self.id, terminal), Ok(())));
    }

    fn observe_and_reclaim(mut self, worker: ThreadId) {
        assert_eq!(self.wake.wait(), worker);
        let terminal = self
            .observer
            .wait()
            .unwrap_or_else(|error| panic!("observe typed admin terminal: {error}"));
        drop(terminal);
        assert_eq!(self.registry.next_reclaim(), Ok(Some(self.id)));
        assert_eq!(
            self.registry.finish_reclaim(self.id),
            Ok(ReclaimStatus::Reclaimed)
        );
    }
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
