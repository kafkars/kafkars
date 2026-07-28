//! One bounded notifier worker shared by transaction initialization and execution.

use std::thread::ThreadId;

use kafka_client_core::TransactionLifecycleTerminal;

use crate::completion::{
    CompletionRegistryError, NotificationTicket, NotifierJoin, PublishTicket, SharedNotifier,
    SharedPublishPort,
};

use super::{
    initialization::RetainedTransactionInitializationOutcome,
    offset_commit::TransactionOffsetCommitResult, send::TransactionSendTerminal,
};

pub(super) const TRANSACTION_OWNER_CAPACITY: usize = 8;
const TRANSACTION_NOTIFICATION_CAPACITY: usize = TRANSACTION_OWNER_CAPACITY
    + TRANSACTION_OWNER_CAPACITY
    + TRANSACTION_OWNER_CAPACITY
    + TRANSACTION_OWNER_CAPACITY;
const TRANSACTION_NOTIFIER_THREAD: &str = "kafka-client-transaction-completion-notifier";

pub(super) enum TransactionPublishTicket {
    Initialization(PublishTicket<RetainedTransactionInitializationOutcome>),
    LifecycleEnd(PublishTicket<TransactionLifecycleTerminal>),
    Send(PublishTicket<TransactionSendTerminal>),
    OffsetCommit(PublishTicket<TransactionOffsetCommitResult>),
}

impl NotificationTicket for TransactionPublishTicket {
    fn publish(self) {
        match self {
            Self::Initialization(ticket) => ticket.publish(),
            Self::LifecycleEnd(ticket) => ticket.publish(),
            Self::Send(ticket) => ticket.publish(),
            Self::OffsetCommit(ticket) => ticket.publish(),
        }
    }
}

pub(super) type TransactionInitializationPublisher =
    SharedPublishPort<RetainedTransactionInitializationOutcome, TransactionPublishTicket>;
pub(super) type TransactionLifecyclePublisher =
    SharedPublishPort<TransactionLifecycleTerminal, TransactionPublishTicket>;
pub(super) type TransactionSendPublisher =
    SharedPublishPort<TransactionSendTerminal, TransactionPublishTicket>;
pub(super) type TransactionOffsetCommitPublisher =
    SharedPublishPort<TransactionOffsetCommitResult, TransactionPublishTicket>;

pub(super) struct TransactionCompletionOwner {
    worker: Option<SharedNotifier<TransactionPublishTicket>>,
}

impl TransactionCompletionOwner {
    pub(super) fn start() -> std::io::Result<Self> {
        Ok(Self {
            worker: Some(SharedNotifier::start(
                TRANSACTION_NOTIFICATION_CAPACITY,
                TRANSACTION_NOTIFIER_THREAD,
            )?),
        })
    }

    pub(super) fn initialization_publisher(
        &self,
    ) -> Result<TransactionInitializationPublisher, CompletionRegistryError> {
        self.worker
            .as_ref()
            .map(|worker| worker.publish_port(TransactionPublishTicket::Initialization))
            .ok_or(CompletionRegistryError::NotifierStopped)
    }

    pub(super) fn lifecycle_publisher(
        &self,
    ) -> Result<TransactionLifecyclePublisher, CompletionRegistryError> {
        self.worker
            .as_ref()
            .map(|worker| worker.publish_port(TransactionPublishTicket::LifecycleEnd))
            .ok_or(CompletionRegistryError::NotifierStopped)
    }

    pub(super) fn send_publisher(
        &self,
    ) -> Result<TransactionSendPublisher, CompletionRegistryError> {
        self.worker
            .as_ref()
            .map(|worker| worker.publish_port(TransactionPublishTicket::Send))
            .ok_or(CompletionRegistryError::NotifierStopped)
    }

    pub(super) fn offset_commit_publisher(
        &self,
    ) -> Result<TransactionOffsetCommitPublisher, CompletionRegistryError> {
        self.worker
            .as_ref()
            .map(|worker| worker.publish_port(TransactionPublishTicket::OffsetCommit))
            .ok_or(CompletionRegistryError::NotifierStopped)
    }

    pub(super) fn thread_id(&self) -> Option<ThreadId> {
        self.worker.as_ref().and_then(SharedNotifier::thread_id)
    }

    pub(super) fn stop(&mut self) -> Result<NotifierJoin, CompletionRegistryError> {
        self.take_join()
            .ok_or(CompletionRegistryError::NotifierStopped)
    }

    pub(super) fn take_join(&mut self) -> Option<NotifierJoin> {
        self.worker.take().map(SharedNotifier::stop)
    }
}

impl Drop for TransactionCompletionOwner {
    fn drop(&mut self) {
        drop(self.take_join());
    }
}
