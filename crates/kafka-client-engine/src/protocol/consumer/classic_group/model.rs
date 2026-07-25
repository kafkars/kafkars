//! Owned generated-type-free facts crossing the classic-group protocol seam.

use core::num::NonZeroI16;
use std::sync::Arc;

use kafka_client_core::{ClassicGeneration, JoinedMemberSlot, TopicId};

/// Exact broker rejection retained without classifying retry behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClassicBrokerRejection {
    throttle_time_ms: u32,
    error_code: NonZeroI16,
}

impl ClassicBrokerRejection {
    pub(super) const fn new(throttle_time_ms: u32, error_code: NonZeroI16) -> Self {
        Self {
            throttle_time_ms,
            error_code,
        }
    }

    pub(crate) const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }

    pub(crate) const fn error_code(self) -> NonZeroI16 {
        self.error_code
    }
}

/// One response member and decoded v0 Range subscription.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ClassicJoinedMember {
    slot: JoinedMemberSlot,
    member: Arc<str>,
    topics: Vec<Arc<str>>,
}

impl ClassicJoinedMember {
    pub(super) const fn new(
        slot: JoinedMemberSlot,
        member: Arc<str>,
        topics: Vec<Arc<str>>,
    ) -> Self {
        Self {
            slot,
            member,
            topics,
        }
    }

    pub(crate) fn into_parts(self) -> (JoinedMemberSlot, Arc<str>, Vec<Arc<str>>) {
        (self.slot, self.member, self.topics)
    }
}

/// Whether the joined member follows or owns the complete assignment plan.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ClassicJoinedRole {
    leader_members: Option<Vec<ClassicJoinedMember>>,
}

impl ClassicJoinedRole {
    pub(super) const fn follower() -> Self {
        Self {
            leader_members: None,
        }
    }

    pub(super) const fn leader(members: Vec<ClassicJoinedMember>) -> Self {
        Self {
            leader_members: Some(members),
        }
    }

    pub(crate) fn into_leader_members(self) -> Option<Vec<ClassicJoinedMember>> {
        self.leader_members
    }
}

/// Correlated successful Join facts ready for candidate staging.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ClassicJoinedGroup {
    throttle_time_ms: u32,
    generation: ClassicGeneration,
    member: Arc<str>,
    role: ClassicJoinedRole,
}

impl ClassicJoinedGroup {
    pub(super) const fn new(
        throttle_time_ms: u32,
        generation: ClassicGeneration,
        member: Arc<str>,
        role: ClassicJoinedRole,
    ) -> Self {
        Self {
            throttle_time_ms,
            generation,
            member,
            role,
        }
    }

    pub(crate) fn into_parts(self) -> (u32, ClassicGeneration, Arc<str>, ClassicJoinedRole) {
        (
            self.throttle_time_ms,
            self.generation,
            self.member,
            self.role,
        )
    }
}

/// One exact Join terminal without transport or retry policy.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ClassicJoinOutcome {
    Rejected(ClassicBrokerRejection),
    Joined(ClassicJoinedGroup),
}

/// One member spelling correlated to a core assignment-plan slot.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ClassicSyncMember {
    slot: JoinedMemberSlot,
    member: Arc<str>,
}

impl ClassicSyncMember {
    pub(crate) const fn new(slot: JoinedMemberSlot, member: Arc<str>) -> Self {
        Self { slot, member }
    }

    pub(super) const fn slot(&self) -> JoinedMemberSlot {
        self.slot
    }

    pub(super) fn member(&self) -> &str {
        &self.member
    }
}

/// One topic spelling correlated to an engine-catalog identity.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ClassicSyncTopic {
    topic_id: TopicId,
    topic: Arc<str>,
}

impl ClassicSyncTopic {
    pub(crate) const fn new(topic_id: TopicId, topic: Arc<str>) -> Self {
        Self { topic_id, topic }
    }

    pub(super) const fn topic_id(&self) -> TopicId {
        self.topic_id
    }

    pub(super) fn topic(&self) -> &str {
        &self.topic
    }
}

/// One decoded topic-partition before catalog identity translation.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct NamedAssignmentPartition {
    topic: Arc<str>,
    partition: i32,
}

impl NamedAssignmentPartition {
    pub(super) const fn new(topic: Arc<str>, partition: i32) -> Self {
        Self { topic, partition }
    }

    #[cfg(test)]
    pub(crate) const fn from_assignment_decode_parts_for_test(
        topic: Arc<str>,
        partition: i32,
    ) -> Self {
        Self { topic, partition }
    }

    pub(crate) fn into_parts(self) -> (Arc<str>, i32) {
        (self.topic, self.partition)
    }

    pub(crate) fn topic(&self) -> &str {
        &self.topic
    }

    pub(crate) const fn partition(&self) -> i32 {
        self.partition
    }
}

/// One exact Sync terminal without transport or retry policy.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ClassicSyncOutcome {
    Rejected(ClassicBrokerRejection),
    Assigned {
        throttle_time_ms: u32,
        partitions: Vec<NamedAssignmentPartition>,
    },
}
