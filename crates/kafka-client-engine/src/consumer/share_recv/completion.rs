//! Dedicated bounded notifier ownership for share receive observation.

use crate::completion::{NotifierJoin, SharedNotificationPort, SharedNotifier};

use super::ShareConsumerRecvTicket;

const SHARE_CONSUMER_RECV_NOTIFICATION_CAPACITY: usize = 1;
const SHARE_CONSUMER_RECV_NOTIFIER_THREAD: &str = "kafka-client-share-consumer-recv-notifier";

pub(crate) type ShareConsumerRecvPublisher =
    SharedNotificationPort<ShareConsumerRecvTicket, ShareConsumerRecvTicket>;

/// Startup resources retained until the registry enters its synchronized shard.
pub(crate) struct ShareConsumerRecvNotificationResources {
    pub(crate) notifier: ShareConsumerRecvNotifier,
    pub(crate) publisher: ShareConsumerRecvPublisher,
}

impl ShareConsumerRecvNotificationResources {
    pub(crate) fn start() -> std::io::Result<Self> {
        let worker = SharedNotifier::start(
            SHARE_CONSUMER_RECV_NOTIFICATION_CAPACITY,
            SHARE_CONSUMER_RECV_NOTIFIER_THREAD,
        )?;
        let publisher = worker.notification_port(identity);
        Ok(Self {
            notifier: ShareConsumerRecvNotifier {
                worker: Some(worker),
            },
            publisher,
        })
    }
}

/// Unique lifecycle owner for the share receive notifier thread.
pub(crate) struct ShareConsumerRecvNotifier {
    worker: Option<SharedNotifier<ShareConsumerRecvTicket>>,
}

impl ShareConsumerRecvNotifier {
    pub(crate) fn stop(&mut self) -> Option<NotifierJoin> {
        self.worker.take().map(SharedNotifier::stop)
    }
}

impl Drop for ShareConsumerRecvNotifier {
    fn drop(&mut self) {
        // Explicit engine shutdown transfers and joins this owner off-reactor.
        // Drop remains leak insurance for partially constructed paths.
        drop(self.stop());
    }
}

const fn identity(ticket: ShareConsumerRecvTicket) -> ShareConsumerRecvTicket {
    ticket
}
