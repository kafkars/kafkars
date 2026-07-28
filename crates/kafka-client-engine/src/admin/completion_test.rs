//! Shared admin completion capacity and typed publication scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Wake, Waker},
    thread::ThreadId,
};

use kafka_client_core::{
    AdminDescribeConsumerGroupsBatch, AdminDescribeConsumerGroupsTerminal,
    AdminListConsumerGroupsBatch, AdminListConsumerGroupsTerminal, AdminListOffsetsBatch,
    AdminListOffsetsTerminal, AlterConsumerGroupOffsetsBatch, AlterConsumerGroupOffsetsTerminal,
    AlterPartitionReassignmentsBatch, AlterPartitionReassignmentsTerminal, ClusterDescription,
    CreatePartitionsTerminal, CreateTopicsTerminal, DeleteConsumerGroupOffsetsBatch,
    DeleteConsumerGroupOffsetsTerminal, DeleteConsumerGroupsBatch, DeleteConsumerGroupsTerminal,
    DeleteRecordsBatch, DeleteRecordsTerminal, DeleteTopicsTerminal, DescribeClusterTerminal,
    DescribeConfigsBatch, DescribeConfigsTerminal, DescribeTopicsTerminal, ElectLeadersBatch,
    ElectLeadersTerminal, IncrementalAlterConfigsBatch, IncrementalAlterConfigsTerminal,
    ListConsumerGroupOffsetsBatch, ListConsumerGroupOffsetsTerminal,
    ListPartitionReassignmentsBatch, ListPartitionReassignmentsTerminal,
    RemoveConsumerGroupMembersBatch, RemoveConsumerGroupMembersTerminal,
};

use crate::completion::{CompletionRegistry, ReclaimStatus};

use super::{
    ADMIN_LIST_OFFSETS_CAPACITY, ALTER_CLIENT_QUOTAS_CAPACITY,
    ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY, ALTER_PARTITION_REASSIGNMENTS_CAPACITY,
    ALTER_REPLICA_LOG_DIRS_CAPACITY, CREATE_ACLS_CAPACITY, CREATE_PARTITIONS_CAPACITY,
    CREATE_TOPICS_CAPACITY, DELETE_ACLS_CAPACITY, DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY,
    DELETE_CONSUMER_GROUPS_CAPACITY, DELETE_RECORDS_CAPACITY, DELETE_TOPICS_CAPACITY,
    DESCRIBE_ACLS_CAPACITY, DESCRIBE_CLIENT_QUOTAS_CAPACITY, DESCRIBE_CLUSTER_CAPACITY,
    DESCRIBE_CONFIGS_CAPACITY, DESCRIBE_CONSUMER_GROUPS_CAPACITY, DESCRIBE_LOG_DIRS_CAPACITY,
    DESCRIBE_TOPICS_CAPACITY, DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY, ELECT_LEADERS_CAPACITY,
    INCREMENTAL_ALTER_CONFIGS_CAPACITY, LIST_CONSUMER_GROUP_OFFSETS_CAPACITY,
    LIST_CONSUMER_GROUPS_CAPACITY, LIST_PARTITION_REASSIGNMENTS_CAPACITY,
    REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY, completion::AdminCompletionNotifier,
    test_support::completion_owner,
};

macro_rules! exercise_terminals {
    ($worker:expr; $($publisher:expr => $terminal:expr),+ $(,)?) => {
        $(exercise_terminal($publisher, $terminal, $worker);)+
    };
}

#[test]
fn one_worker_publishes_every_concrete_admin_terminal_off_reactor() {
    let reactor = std::thread::current().id();
    let (mut notifier, ports) = completion_owner();
    let worker = notifier
        .thread_id()
        .unwrap_or_else(|| panic!("shared admin notifier must own one worker"));
    assert_ne!(worker, reactor);

    exercise_terminals! {
        worker;
        ports.create_topics => CreateTopicsTerminal::Topics(Vec::new()),
        ports.delete_topics => DeleteTopicsTerminal::Topics(Vec::new()),
        ports.describe_cluster => DescribeClusterTerminal::Cluster(ClusterDescription::new(
            String::from("cluster"),
            None,
            Vec::new(),
        )),
        ports.create_partitions => CreatePartitionsTerminal::Topics(Vec::new()),
        ports.describe_topics => DescribeTopicsTerminal::Topics(Vec::new()),
        ports.describe_configs => DescribeConfigsTerminal::Configs(
            DescribeConfigsBatch::new(0, Vec::new())
        ),
        ports.incremental_alter_configs => IncrementalAlterConfigsTerminal::Configs(
            IncrementalAlterConfigsBatch::new(0, Vec::new())
        ),
        ports.list_consumer_group_offsets => ListConsumerGroupOffsetsTerminal::Offsets(
            ListConsumerGroupOffsetsBatch::new(
                0,
                Vec::new(),
            )
        ),
        ports.delete_consumer_group_offsets => DeleteConsumerGroupOffsetsTerminal::Deleted(
            DeleteConsumerGroupOffsetsBatch::new(
                0,
                Vec::new(),
            )
        ),
        ports.delete_consumer_groups => DeleteConsumerGroupsTerminal::Deleted(
            DeleteConsumerGroupsBatch::new(0, Vec::new())
        ),
        ports.alter_consumer_group_offsets => AlterConsumerGroupOffsetsTerminal::Altered(
            AlterConsumerGroupOffsetsBatch::new(
                0,
                Vec::new(),
            )
        ),
        ports.admin_list_offsets => AdminListOffsetsTerminal::Listed(
            AdminListOffsetsBatch::new(0, Vec::new())
        ),
        ports.list_partition_reassignments => ListPartitionReassignmentsTerminal::Reassignments(
            ListPartitionReassignmentsBatch::new(
                0,
                Vec::new(),
            )
        ),
        ports.alter_partition_reassignments => AlterPartitionReassignmentsTerminal::Altered(
            AlterPartitionReassignmentsBatch::new(
                0,
                Vec::new(),
            )
        ),
        ports.elect_leaders => ElectLeadersTerminal::Elected(
            ElectLeadersBatch::new(0, Vec::new())
        ),
        ports.delete_records => DeleteRecordsTerminal::Deleted(
            DeleteRecordsBatch::new(0, Vec::new())
        ),
        ports.describe_consumer_groups => AdminDescribeConsumerGroupsTerminal::Described(
            AdminDescribeConsumerGroupsBatch::new(
                0,
                Vec::new(),
            )
        ),
        ports.list_consumer_groups => AdminListConsumerGroupsTerminal::Listed(
            AdminListConsumerGroupsBatch::new(
                0,
                Vec::new(),
                Vec::new(),
            )
        ),
        ports.remove_consumer_group_members => RemoveConsumerGroupMembersTerminal::Removed(
            RemoveConsumerGroupMembersBatch::new(
                0,
                Vec::new(),
            )
        ),
    }

    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop shared notifier: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}

fn exercise_terminal<T, P>(publisher: P, terminal: T, worker: ThreadId)
where
    T: Send + 'static,
    P: crate::completion::CompletionPublisher<T>,
{
    let mut pending = PendingTerminal::new(publisher);
    pending.publish(terminal);
    pending.observe_and_reclaim(worker);
}

#[test]
fn shared_capacity_is_the_sum_of_the_closed_admin_ticket_set() {
    assert_eq!(
        AdminCompletionNotifier::capacity_for_test(),
        CREATE_TOPICS_CAPACITY
            + DELETE_TOPICS_CAPACITY
            + DESCRIBE_CLUSTER_CAPACITY
            + CREATE_PARTITIONS_CAPACITY
            + DESCRIBE_TOPICS_CAPACITY
            + DESCRIBE_CONFIGS_CAPACITY
            + INCREMENTAL_ALTER_CONFIGS_CAPACITY
            + LIST_CONSUMER_GROUP_OFFSETS_CAPACITY
            + DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY
            + DELETE_CONSUMER_GROUPS_CAPACITY
            + ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY
            + ADMIN_LIST_OFFSETS_CAPACITY
            + LIST_PARTITION_REASSIGNMENTS_CAPACITY
            + ALTER_PARTITION_REASSIGNMENTS_CAPACITY
            + ELECT_LEADERS_CAPACITY
            + DELETE_RECORDS_CAPACITY
            + DESCRIBE_CONSUMER_GROUPS_CAPACITY
            + LIST_CONSUMER_GROUPS_CAPACITY
            + REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY
            + DESCRIBE_LOG_DIRS_CAPACITY
            + ALTER_REPLICA_LOG_DIRS_CAPACITY
            + DESCRIBE_ACLS_CAPACITY
            + DESCRIBE_CLIENT_QUOTAS_CAPACITY
            + ALTER_CLIENT_QUOTAS_CAPACITY
            + DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY
            + CREATE_ACLS_CAPACITY
            + DELETE_ACLS_CAPACITY
    );
}

#[test]
fn describe_topics_is_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        AdminCompletionNotifier::capacity_for_test().checked_sub(
            CREATE_TOPICS_CAPACITY
                + DELETE_TOPICS_CAPACITY
                + DESCRIBE_CLUSTER_CAPACITY
                + CREATE_PARTITIONS_CAPACITY
                + DESCRIBE_CONFIGS_CAPACITY
                + INCREMENTAL_ALTER_CONFIGS_CAPACITY
                + LIST_CONSUMER_GROUP_OFFSETS_CAPACITY
                + DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY
                + DELETE_CONSUMER_GROUPS_CAPACITY
                + ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY
                + ADMIN_LIST_OFFSETS_CAPACITY
                + LIST_PARTITION_REASSIGNMENTS_CAPACITY
                + ALTER_PARTITION_REASSIGNMENTS_CAPACITY
                + ELECT_LEADERS_CAPACITY
                + DELETE_RECORDS_CAPACITY
                + DESCRIBE_CONSUMER_GROUPS_CAPACITY
                + LIST_CONSUMER_GROUPS_CAPACITY
                + REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY
                + DESCRIBE_LOG_DIRS_CAPACITY
                + ALTER_REPLICA_LOG_DIRS_CAPACITY
                + DESCRIBE_ACLS_CAPACITY
                + DESCRIBE_CLIENT_QUOTAS_CAPACITY
                + ALTER_CLIENT_QUOTAS_CAPACITY
                + DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY
                + CREATE_ACLS_CAPACITY
                + DELETE_ACLS_CAPACITY
        ),
        Some(DESCRIBE_TOPICS_CAPACITY)
    );
}

#[test]
fn create_partitions_is_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        AdminCompletionNotifier::capacity_for_test().checked_sub(
            CREATE_TOPICS_CAPACITY
                + DELETE_TOPICS_CAPACITY
                + DESCRIBE_CLUSTER_CAPACITY
                + DESCRIBE_TOPICS_CAPACITY
                + DESCRIBE_CONFIGS_CAPACITY
                + INCREMENTAL_ALTER_CONFIGS_CAPACITY
                + LIST_CONSUMER_GROUP_OFFSETS_CAPACITY
                + DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY
                + DELETE_CONSUMER_GROUPS_CAPACITY
                + ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY
                + ADMIN_LIST_OFFSETS_CAPACITY
                + LIST_PARTITION_REASSIGNMENTS_CAPACITY
                + ALTER_PARTITION_REASSIGNMENTS_CAPACITY
                + ELECT_LEADERS_CAPACITY
                + DELETE_RECORDS_CAPACITY
                + DESCRIBE_CONSUMER_GROUPS_CAPACITY
                + LIST_CONSUMER_GROUPS_CAPACITY
                + REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY
                + DESCRIBE_LOG_DIRS_CAPACITY
                + ALTER_REPLICA_LOG_DIRS_CAPACITY
                + DESCRIBE_ACLS_CAPACITY
                + DESCRIBE_CLIENT_QUOTAS_CAPACITY
                + ALTER_CLIENT_QUOTAS_CAPACITY
                + DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY
                + CREATE_ACLS_CAPACITY
                + DELETE_ACLS_CAPACITY
        ),
        Some(CREATE_PARTITIONS_CAPACITY)
    );
}

#[test]
fn describe_configs_is_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        AdminCompletionNotifier::capacity_for_test().checked_sub(
            CREATE_TOPICS_CAPACITY
                + DELETE_TOPICS_CAPACITY
                + DESCRIBE_CLUSTER_CAPACITY
                + CREATE_PARTITIONS_CAPACITY
                + DESCRIBE_TOPICS_CAPACITY
                + INCREMENTAL_ALTER_CONFIGS_CAPACITY
                + LIST_CONSUMER_GROUP_OFFSETS_CAPACITY
                + DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY
                + DELETE_CONSUMER_GROUPS_CAPACITY
                + ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY
                + ADMIN_LIST_OFFSETS_CAPACITY
                + LIST_PARTITION_REASSIGNMENTS_CAPACITY
                + ALTER_PARTITION_REASSIGNMENTS_CAPACITY
                + ELECT_LEADERS_CAPACITY
                + DELETE_RECORDS_CAPACITY
                + DESCRIBE_CONSUMER_GROUPS_CAPACITY
                + LIST_CONSUMER_GROUPS_CAPACITY
                + REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY
                + DESCRIBE_LOG_DIRS_CAPACITY
                + ALTER_REPLICA_LOG_DIRS_CAPACITY
                + DESCRIBE_ACLS_CAPACITY
                + DESCRIBE_CLIENT_QUOTAS_CAPACITY
                + ALTER_CLIENT_QUOTAS_CAPACITY
                + DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY
                + CREATE_ACLS_CAPACITY
                + DELETE_ACLS_CAPACITY
        ),
        Some(DESCRIBE_CONFIGS_CAPACITY)
    );
}

#[test]
fn incremental_alter_configs_is_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        AdminCompletionNotifier::capacity_for_test().checked_sub(
            CREATE_TOPICS_CAPACITY
                + DELETE_TOPICS_CAPACITY
                + DESCRIBE_CLUSTER_CAPACITY
                + CREATE_PARTITIONS_CAPACITY
                + DESCRIBE_TOPICS_CAPACITY
                + DESCRIBE_CONFIGS_CAPACITY
                + LIST_CONSUMER_GROUP_OFFSETS_CAPACITY
                + DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY
                + DELETE_CONSUMER_GROUPS_CAPACITY
                + ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY
                + ADMIN_LIST_OFFSETS_CAPACITY
                + LIST_PARTITION_REASSIGNMENTS_CAPACITY
                + ALTER_PARTITION_REASSIGNMENTS_CAPACITY
                + ELECT_LEADERS_CAPACITY
                + DELETE_RECORDS_CAPACITY
                + DESCRIBE_CONSUMER_GROUPS_CAPACITY
                + LIST_CONSUMER_GROUPS_CAPACITY
                + REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY
                + DESCRIBE_LOG_DIRS_CAPACITY
                + ALTER_REPLICA_LOG_DIRS_CAPACITY
                + DESCRIBE_ACLS_CAPACITY
                + DESCRIBE_CLIENT_QUOTAS_CAPACITY
                + ALTER_CLIENT_QUOTAS_CAPACITY
                + DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY
                + CREATE_ACLS_CAPACITY
                + DELETE_ACLS_CAPACITY
        ),
        Some(INCREMENTAL_ALTER_CONFIGS_CAPACITY)
    );
}

#[test]
fn group_offsets_is_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        AdminCompletionNotifier::capacity_for_test().checked_sub(
            CREATE_TOPICS_CAPACITY
                + DELETE_TOPICS_CAPACITY
                + DESCRIBE_CLUSTER_CAPACITY
                + CREATE_PARTITIONS_CAPACITY
                + DESCRIBE_TOPICS_CAPACITY
                + DESCRIBE_CONFIGS_CAPACITY
                + INCREMENTAL_ALTER_CONFIGS_CAPACITY
                + DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY
                + DELETE_CONSUMER_GROUPS_CAPACITY
                + ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY
                + ADMIN_LIST_OFFSETS_CAPACITY
                + LIST_PARTITION_REASSIGNMENTS_CAPACITY
                + ALTER_PARTITION_REASSIGNMENTS_CAPACITY
                + ELECT_LEADERS_CAPACITY
                + DELETE_RECORDS_CAPACITY
                + DESCRIBE_CONSUMER_GROUPS_CAPACITY
                + LIST_CONSUMER_GROUPS_CAPACITY
                + REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY
                + DESCRIBE_LOG_DIRS_CAPACITY
                + ALTER_REPLICA_LOG_DIRS_CAPACITY
                + DESCRIBE_ACLS_CAPACITY
                + DESCRIBE_CLIENT_QUOTAS_CAPACITY
                + ALTER_CLIENT_QUOTAS_CAPACITY
                + DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY
                + CREATE_ACLS_CAPACITY
                + DELETE_ACLS_CAPACITY
        ),
        Some(LIST_CONSUMER_GROUP_OFFSETS_CAPACITY)
    );
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
