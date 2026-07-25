//! Bounded bytes-free facts normalized from classic consumer-group protocol data.

use crate::{MemberId, TopicId};

use super::{JoinedMemberSlot, MemberRank};

/// Maximum members retained from one classic Join response.
pub(super) const MAX_CLASSIC_GROUP_MEMBERS: usize = 64;

/// Maximum unique topics retained across one classic group.
pub(super) const MAX_CLASSIC_GROUP_TOPICS: usize = 64;

/// Maximum subscribed topics retained for one classic member.
pub(super) const MAX_CLASSIC_TOPICS_PER_MEMBER: usize = 64;

/// Lifecycle stage of one deterministic classic-group owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicGroupPhase {
    /// No Join and Sync cycle is active.
    Dormant,
    /// One exact Join request is outstanding.
    Joining,
    /// A leader waits for bounded topic partition counts.
    AwaitingPartitionCounts,
    /// One exact Sync request is outstanding.
    Syncing,
    /// Matching Sync success installed the live assignment.
    Stable,
    /// The active cycle or assignment was terminally lost.
    Lost,
    /// Admission is permanently closed.
    Closed,
}

/// Core-owned classic assignment protocol selected for one membership cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicProtocol {
    /// Dynamic, non-rack-aware Kafka Range assignment.
    Range,
}

/// Ordered unique scalar topics subscribed by one classic member.
#[derive(Debug, Eq, PartialEq)]
pub struct ClassicSubscription {
    topics: Vec<TopicId>,
}

impl ClassicSubscription {
    /// Validates the bounded, strictly ordered topic set.
    pub fn try_new(topics: Vec<TopicId>) -> Result<Self, ClassicSubscriptionError> {
        if topics.len() > MAX_CLASSIC_TOPICS_PER_MEMBER {
            return Err(ClassicSubscriptionError::TooManyTopics);
        }
        for pair in topics.windows(2) {
            if pair[0] == pair[1] {
                return Err(ClassicSubscriptionError::DuplicateTopic(pair[0]));
            }
            if pair[0] > pair[1] {
                return Err(ClassicSubscriptionError::OutOfOrder);
            }
        }
        Ok(Self { topics })
    }

    /// Borrows the engine-catalog topic identities in deterministic order.
    pub fn topics(&self) -> &[TopicId] {
        &self.topics
    }
}

/// Structural rejection of one member subscription.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicSubscriptionError {
    /// The normalized subscription exceeded its reviewed bound.
    TooManyTopics,
    /// A topic appeared more than once.
    DuplicateTopic(TopicId),
    /// Topic identities were not presented in ascending order.
    OutOfOrder,
}

/// One normalized member in the ordering chosen from Kafka member identifiers.
#[derive(Debug, Eq, PartialEq)]
pub struct ClassicJoinMember {
    slot: JoinedMemberSlot,
    member_id: MemberId,
    rank: MemberRank,
    subscription: ClassicSubscription,
}

impl ClassicJoinMember {
    /// Creates one already-normalized scalar member fact.
    pub const fn new(
        slot: JoinedMemberSlot,
        member_id: MemberId,
        rank: MemberRank,
        subscription: ClassicSubscription,
    ) -> Self {
        Self {
            slot,
            member_id,
            rank,
            subscription,
        }
    }

    /// Returns the engine correlation slot.
    pub const fn slot(&self) -> JoinedMemberSlot {
        self.slot
    }

    /// Returns the engine-catalog member identity.
    pub const fn member_id(&self) -> MemberId {
        self.member_id
    }

    /// Returns the ordering rank derived from the Kafka member identifier.
    pub const fn rank(&self) -> MemberRank {
        self.rank
    }

    /// Borrows the ordered topic subscription.
    pub const fn subscription(&self) -> &ClassicSubscription {
        &self.subscription
    }
}

/// Bounded members from one leader Join response, ordered by member rank.
#[derive(Debug, Eq, PartialEq)]
pub struct ClassicJoinMembers {
    members: Vec<ClassicJoinMember>,
}

impl ClassicJoinMembers {
    /// Validates bounded members and unique, ascending slots, IDs, and ranks.
    pub fn try_new(members: Vec<ClassicJoinMember>) -> Result<Self, ClassicJoinMembersError> {
        if members.is_empty() {
            return Err(ClassicJoinMembersError::Empty);
        }
        if members.len() > MAX_CLASSIC_GROUP_MEMBERS {
            return Err(ClassicJoinMembersError::TooManyMembers);
        }
        for (index, member) in members.iter().enumerate() {
            if index > 0 && members[index - 1].rank() >= member.rank() {
                return Err(ClassicJoinMembersError::RankOrder);
            }
            if members[..index]
                .iter()
                .any(|prior| prior.slot() == member.slot())
            {
                return Err(ClassicJoinMembersError::DuplicateSlot(member.slot()));
            }
            if members[..index]
                .iter()
                .any(|prior| prior.member_id() == member.member_id())
            {
                return Err(ClassicJoinMembersError::DuplicateMember(member.member_id()));
            }
        }
        Ok(Self { members })
    }

    /// Borrows members in Kafka member-identifier order.
    pub fn members(&self) -> &[ClassicJoinMember] {
        &self.members
    }
}

/// Structural rejection of one normalized leader member set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicJoinMembersError {
    /// A leader response contained no members.
    Empty,
    /// The leader response exceeded its reviewed bound.
    TooManyMembers,
    /// Two members used one engine response slot.
    DuplicateSlot(JoinedMemberSlot),
    /// Two entries identified one normalized group member.
    DuplicateMember(MemberId),
    /// Member ranks were duplicated or not ascending.
    RankOrder,
}

/// One topic and its authoritative nonnegative partition count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TopicPartitionCount {
    topic_id: TopicId,
    count: u32,
}

impl TopicPartitionCount {
    /// Creates one scalar partition-count fact.
    pub const fn new(topic_id: TopicId, count: u32) -> Self {
        Self { topic_id, count }
    }

    /// Returns the engine-catalog topic identity.
    pub const fn topic_id(self) -> TopicId {
        self.topic_id
    }

    /// Returns the number of available partitions.
    pub const fn count(self) -> u32 {
        self.count
    }
}
