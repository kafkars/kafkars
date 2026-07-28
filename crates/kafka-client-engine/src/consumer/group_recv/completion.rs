//! Dedicated bounded notifier ownership for classic-group receive observation.

use crate::completion::{NotifierJoin, SharedNotificationPort, SharedNotifier};

use super::GroupConsumerRecvTicket;

const GROUP_CONSUMER_RECV_NOTIFICATION_CAPACITY: usize = 1;
const GROUP_CONSUMER_RECV_NOTIFIER_THREAD: &str = "kafka-client-group-consumer-recv-notifier";

pub(crate) type GroupConsumerRecvPublisher =
    SharedNotificationPort<GroupConsumerRecvTicket, GroupConsumerRecvTicket>;

/// Startup resources retained until the registry enters its synchronized shard.
pub(crate) struct GroupConsumerRecvNotificationResources {
    pub(crate) notifier: GroupConsumerRecvNotifier,
    pub(crate) publisher: GroupConsumerRecvPublisher,
}

impl GroupConsumerRecvNotificationResources {
    pub(crate) fn start() -> std::io::Result<Self> {
        let worker = SharedNotifier::start(
            GROUP_CONSUMER_RECV_NOTIFICATION_CAPACITY,
            GROUP_CONSUMER_RECV_NOTIFIER_THREAD,
        )?;
        let publisher = worker.notification_port(identity);
        Ok(Self {
            notifier: GroupConsumerRecvNotifier {
                worker: Some(worker),
            },
            publisher,
        })
    }
}

/// Unique lifecycle owner for the group receive notifier thread.
pub(crate) struct GroupConsumerRecvNotifier {
    worker: Option<SharedNotifier<GroupConsumerRecvTicket>>,
}

impl GroupConsumerRecvNotifier {
    pub(crate) fn stop(&mut self) -> Option<NotifierJoin> {
        self.worker.take().map(SharedNotifier::stop)
    }
}

impl Drop for GroupConsumerRecvNotifier {
    fn drop(&mut self) {
        // Explicit engine shutdown transfers and joins this owner off-reactor.
        // Drop remains leak insurance for partially constructed test/start paths.
        drop(self.stop());
    }
}

const fn identity(ticket: GroupConsumerRecvTicket) -> GroupConsumerRecvTicket {
    ticket
}
