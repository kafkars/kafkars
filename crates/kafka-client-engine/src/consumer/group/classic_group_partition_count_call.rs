//! Exact cycle-, topic-, and deadline-fenced ownership of one topic-view call.

use std::sync::Arc;

use kafka_client_core::{GroupId, MembershipCycle, TopicId};

use crate::{
    clock::OperationDeadline,
    driver::{
        DriverOwner, TopicPartitionCountAdmissionFailure, TopicPartitionCountCall,
        TopicPartitionCountFact, TopicPartitionCountFailure,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ClassicGroupPartitionCountCallIdentity {
    group_id: GroupId,
    cycle: MembershipCycle,
    topic_id: TopicId,
    deadline: OperationDeadline,
}

/// One accepted driver-owned lookup for the next exact ordered topic.
#[must_use = "an accepted partition-count lookup must settle or recover"]
#[expect(
    clippy::struct_field_names,
    reason = "qualified field names make each partition-count ownership role explicit"
)]
pub(super) struct ClassicGroupPartitionCountCall {
    partition_count_identity: ClassicGroupPartitionCountCallIdentity,
    partition_count_topic: Arc<str>,
    partition_count_driver_call: TopicPartitionCountCall,
}

impl ClassicGroupPartitionCountCall {
    pub(super) fn submit(
        driver: &DriverOwner,
        identity: ClassicGroupPartitionCountCallIdentity,
        topic: Arc<str>,
    ) -> Result<Self, TopicPartitionCountAdmissionFailure> {
        let call = TopicPartitionCountCall::submit(driver, &topic, identity.deadline.transport())?;
        Ok(Self {
            partition_count_identity: identity,
            partition_count_topic: topic,
            partition_count_driver_call: call,
        })
    }

    pub(super) const fn identity(&self) -> ClassicGroupPartitionCountCallIdentity {
        self.partition_count_identity
    }

    pub(super) fn topic(&self) -> &Arc<str> {
        &self.partition_count_topic
    }

    pub(super) fn try_terminal(
        &mut self,
    ) -> Option<Result<TopicPartitionCountFact, TopicPartitionCountFailure>> {
        self.partition_count_driver_call.try_terminal()
    }

    pub(super) fn discard_after_driver_shutdown(self) {
        self.partition_count_driver_call
            .discard_after_driver_shutdown();
    }
}

impl ClassicGroupPartitionCountCallIdentity {
    pub(super) const fn new(
        group_id: GroupId,
        cycle: MembershipCycle,
        topic_id: TopicId,
        deadline: OperationDeadline,
    ) -> Self {
        Self {
            group_id,
            cycle,
            topic_id,
            deadline,
        }
    }

    pub(super) const fn group_id(self) -> GroupId {
        self.group_id
    }

    pub(super) const fn cycle(self) -> MembershipCycle {
        self.cycle
    }

    pub(super) const fn topic_id(self) -> TopicId {
        self.topic_id
    }

    pub(super) const fn deadline(self) -> OperationDeadline {
        self.deadline
    }
}
