//! Exact topic-, name-, and deadline-fenced metadata lookup for KIP-848.

use std::sync::Arc;

use kafka_client_core::TopicId;

use crate::{
    clock::OperationDeadline,
    driver::{
        DriverOwner, TopicPartitionCountAdmissionFailure, TopicPartitionCountCall,
        TopicPartitionCountFact, TopicPartitionCountFailure,
    },
};

/// One accepted broker topic-identity lookup owned by a modern group entry.
#[must_use = "an accepted topic-identity lookup must settle or recover"]
pub(super) struct ConsumerGroupTopicIdentityCall {
    topic_id: TopicId,
    topic: Arc<str>,
    deadline: OperationDeadline,
    call: TopicPartitionCountCall,
}

impl ConsumerGroupTopicIdentityCall {
    pub(super) fn submit(
        driver: &DriverOwner,
        topic_id: TopicId,
        topic: Arc<str>,
        deadline: OperationDeadline,
    ) -> Result<Self, TopicPartitionCountAdmissionFailure> {
        let call = TopicPartitionCountCall::submit(driver, &topic, deadline.transport())?;
        Ok(Self {
            topic_id,
            topic,
            deadline,
            call,
        })
    }

    pub(super) const fn topic_id(&self) -> TopicId {
        self.topic_id
    }

    pub(super) fn topic(&self) -> &Arc<str> {
        &self.topic
    }

    pub(super) const fn deadline(&self) -> OperationDeadline {
        self.deadline
    }

    pub(super) fn try_terminal(
        &mut self,
    ) -> Option<Result<TopicPartitionCountFact, TopicPartitionCountFailure>> {
        self.call.try_terminal()
    }

    pub(super) fn discard_after_driver_shutdown(self) {
        self.call.discard_after_driver_shutdown();
    }
}
