//! One bounded notifier worker shared only by concrete admin terminal owners.

use std::thread::ThreadId;

use kafka_client_core::{
    CreatePartitionsTerminal, CreateTopicsTerminal, DeleteTopicsTerminal, DescribeClusterTerminal,
    DescribeConfigsTerminal, DescribeTopicsTerminal,
};

use crate::completion::{
    CompletionRegistryError, NotificationTicket, NotifierJoin, PublishTicket, SharedNotifier,
    SharedPublishPort,
};

use super::{
    CREATE_PARTITIONS_CAPACITY, CREATE_TOPICS_CAPACITY, DELETE_TOPICS_CAPACITY,
    DESCRIBE_CLUSTER_CAPACITY, DESCRIBE_CONFIGS_CAPACITY, DESCRIBE_TOPICS_CAPACITY,
};

const ADMIN_NOTIFIER_THREAD: &str = "kafka-client-admin-completion-notifier";
const ADMIN_NOTIFICATION_CAPACITY: usize = CREATE_TOPICS_CAPACITY
    + DELETE_TOPICS_CAPACITY
    + DESCRIBE_CLUSTER_CAPACITY
    + CREATE_PARTITIONS_CAPACITY
    + DESCRIBE_TOPICS_CAPACITY
    + DESCRIBE_CONFIGS_CAPACITY;

/// Closed allocation-free set of terminal tickets accepted by the admin worker.
pub(crate) enum AdminPublishTicket {
    CreateTopics(PublishTicket<CreateTopicsTerminal>),
    DeleteTopics(PublishTicket<DeleteTopicsTerminal>),
    DescribeCluster(PublishTicket<DescribeClusterTerminal>),
    CreatePartitions(PublishTicket<CreatePartitionsTerminal>),
    DescribeTopics(PublishTicket<DescribeTopicsTerminal>),
    DescribeConfigs(PublishTicket<DescribeConfigsTerminal>),
}

impl NotificationTicket for AdminPublishTicket {
    fn publish(self) {
        match self {
            Self::CreateTopics(ticket) => ticket.publish(),
            Self::DeleteTopics(ticket) => ticket.publish(),
            Self::DescribeCluster(ticket) => ticket.publish(),
            Self::CreatePartitions(ticket) => ticket.publish(),
            Self::DescribeTopics(ticket) => ticket.publish(),
            Self::DescribeConfigs(ticket) => ticket.publish(),
        }
    }
}

pub(crate) type CreateTopicsPublisher = SharedPublishPort<CreateTopicsTerminal, AdminPublishTicket>;
pub(crate) type DeleteTopicsPublisher = SharedPublishPort<DeleteTopicsTerminal, AdminPublishTicket>;
pub(crate) type DescribeClusterPublisher =
    SharedPublishPort<DescribeClusterTerminal, AdminPublishTicket>;
pub(crate) type CreatePartitionsPublisher =
    SharedPublishPort<CreatePartitionsTerminal, AdminPublishTicket>;
pub(crate) type DescribeTopicsPublisher =
    SharedPublishPort<DescribeTopicsTerminal, AdminPublishTicket>;
pub(crate) type DescribeConfigsPublisher =
    SharedPublishPort<DescribeConfigsTerminal, AdminPublishTicket>;

/// Exact typed ports issued once with the shared worker.
pub(crate) struct AdminCompletionPorts {
    pub(crate) create_topics: CreateTopicsPublisher,
    pub(crate) delete_topics: DeleteTopicsPublisher,
    pub(crate) describe_cluster: DescribeClusterPublisher,
    pub(crate) create_partitions: CreatePartitionsPublisher,
    pub(crate) describe_topics: DescribeTopicsPublisher,
    pub(crate) describe_configs: DescribeConfigsPublisher,
}

/// Unique lifecycle owner for the one shared admin notifier.
pub(crate) struct AdminCompletionNotifier {
    worker: Option<SharedNotifier<AdminPublishTicket>>,
}

impl AdminCompletionNotifier {
    pub(crate) fn start() -> std::io::Result<(Self, AdminCompletionPorts)> {
        let worker = SharedNotifier::start(ADMIN_NOTIFICATION_CAPACITY, ADMIN_NOTIFIER_THREAD)?;
        let ports = AdminCompletionPorts {
            create_topics: worker.publish_port(AdminPublishTicket::CreateTopics),
            delete_topics: worker.publish_port(AdminPublishTicket::DeleteTopics),
            describe_cluster: worker.publish_port(AdminPublishTicket::DescribeCluster),
            create_partitions: worker.publish_port(AdminPublishTicket::CreatePartitions),
            describe_topics: worker.publish_port(AdminPublishTicket::DescribeTopics),
            describe_configs: worker.publish_port(AdminPublishTicket::DescribeConfigs),
        };
        Ok((
            Self {
                worker: Some(worker),
            },
            ports,
        ))
    }

    pub(crate) fn stop(&mut self) -> Result<NotifierJoin, CompletionRegistryError> {
        self.take_join()
            .ok_or(CompletionRegistryError::NotifierStopped)
    }

    pub(crate) fn take_join(&mut self) -> Option<NotifierJoin> {
        self.worker.take().map(SharedNotifier::stop)
    }

    pub(crate) fn thread_id(&self) -> Option<ThreadId> {
        self.worker.as_ref().and_then(SharedNotifier::thread_id)
    }

    #[cfg(test)]
    pub(super) const fn capacity_for_test() -> usize {
        ADMIN_NOTIFICATION_CAPACITY
    }
}
