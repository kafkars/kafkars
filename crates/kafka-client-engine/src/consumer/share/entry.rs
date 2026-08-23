//! One bounded share-member registration before broker membership execution.

use std::sync::Arc;

use kafka_client_core::{GroupId, MemberId, ShareGroupHeartbeatPolicy, TopicId};
use ring::rand::{SecureRandom, SystemRandom};

use super::topic_identity_call::ShareTopicIdentityCall;
use super::{ShareMembershipInterpreter, catalog::ShareTopicIdentity};
use crate::clock::DeadlineCapture;

pub(super) const SHARE_TOPIC_CAPACITY: usize = 32;
pub(super) const SHARE_NAME_BYTE_LIMIT: usize = 249;
pub(super) const SHARE_HEARTBEAT_ATTEMPT_TIMEOUT_TICKS: u64 = 10_000_000_000;

pub(super) struct ShareConsumerEntry {
    group_id: GroupId,
    member_id: MemberId,
    group: Arc<str>,
    member: Arc<str>,
    rack: Option<Arc<str>>,
    topics: Vec<Arc<str>>,
    pub(super) resolved_topics: Vec<ShareTopicIdentity>,
    pub(super) start: Option<DeadlineCapture>,
    pub(super) membership: Option<ShareMembershipInterpreter>,
    pub(super) topic_call: Option<ShareTopicIdentityCall>,
    pub(super) fault: Option<kafka_client_core::ShareGroupHeartbeatFailure>,
}

impl ShareConsumerEntry {
    pub(super) fn try_new(
        group_id: GroupId,
        group: Arc<str>,
        rack: Option<Arc<str>>,
        topics: Vec<Arc<str>>,
    ) -> Result<Self, ShareConsumerEntryBuildFailure> {
        if let Err(kind) = validate_names(&group, rack.as_deref(), &topics) {
            return Err(ShareConsumerEntryBuildFailure {
                kind,
                group,
                rack,
                topics,
            });
        }
        let Some(member_id) = MemberId::try_from_raw(group_id.get()) else {
            return Err(ShareConsumerEntryBuildFailure {
                kind: ShareConsumerEntryBuildError::IdentityExhausted,
                group,
                rack,
                topics,
            });
        };
        let Ok(member) = member_spelling() else {
            return Err(ShareConsumerEntryBuildFailure {
                kind: ShareConsumerEntryBuildError::MemberIdentity,
                group,
                rack,
                topics,
            });
        };
        let mut resolved_topics = Vec::new();
        if resolved_topics.try_reserve_exact(topics.len()).is_err() {
            return Err(ShareConsumerEntryBuildFailure {
                kind: ShareConsumerEntryBuildError::Allocation,
                group,
                rack,
                topics,
            });
        }
        Ok(Self {
            group_id,
            member_id,
            group,
            member,
            rack,
            topics,
            resolved_topics,
            start: None,
            membership: None,
            topic_call: None,
            fault: None,
        })
    }

    pub(super) const fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub(super) const fn member_id(&self) -> MemberId {
        self.member_id
    }

    pub(super) fn group(&self) -> &Arc<str> {
        &self.group
    }

    pub(super) fn member(&self) -> &Arc<str> {
        &self.member
    }

    pub(super) fn rack(&self) -> Option<&Arc<str>> {
        self.rack.as_ref()
    }

    pub(super) fn topics(&self) -> &[Arc<str>] {
        &self.topics
    }

    pub(super) fn local_topic_id(&self, index: usize) -> Option<TopicId> {
        let raw = u64::try_from(index).ok()?.checked_add(1)?;
        self.topics.get(index).map(|_topic| TopicId::from_raw(raw))
    }

    pub(super) fn retained_name_bytes(&self) -> usize {
        self.group.len()
            + self.member.len()
            + self.rack.as_ref().map_or(0, |rack| rack.len())
            + self.topics.iter().map(|topic| topic.len()).sum::<usize>()
    }

    pub(super) fn policy() -> ShareGroupHeartbeatPolicy {
        ShareGroupHeartbeatPolicy::try_new(SHARE_HEARTBEAT_ATTEMPT_TIMEOUT_TICKS)
            .unwrap_or_else(|_| unreachable!("positive share heartbeat attempt timeout"))
    }

    pub(super) fn begin(&mut self, capture: DeadlineCapture) -> Result<(), ()> {
        if self.start.is_some() || self.membership.is_some() || self.fault.is_some() {
            return Err(());
        }
        self.start = Some(capture);
        Ok(())
    }

    pub(super) fn into_registration_parts(self) -> (Arc<str>, Option<Arc<str>>, Vec<Arc<str>>) {
        let Self {
            group,
            rack,
            topics,
            member,
            resolved_topics,
            start,
            membership,
            topic_call,
            fault,
            ..
        } = self;
        drop((
            member,
            resolved_topics,
            start,
            membership,
            topic_call,
            fault,
        ));
        (group, rack, topics)
    }
}

pub(super) struct ShareConsumerEntryBuildFailure {
    pub(super) kind: ShareConsumerEntryBuildError,
    pub(super) group: Arc<str>,
    pub(super) rack: Option<Arc<str>>,
    pub(super) topics: Vec<Arc<str>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareConsumerEntryBuildError {
    EmptyGroup,
    EmptyRack,
    EmptySubscription,
    EmptyTopic,
    NameTooLong,
    TopicCapacity,
    DuplicateTopic,
    IdentityExhausted,
    MemberIdentity,
    Allocation,
}

fn validate_names(
    group: &str,
    rack: Option<&str>,
    topics: &[Arc<str>],
) -> Result<(), ShareConsumerEntryBuildError> {
    if group.is_empty() {
        return Err(ShareConsumerEntryBuildError::EmptyGroup);
    }
    if group.len() > SHARE_NAME_BYTE_LIMIT {
        return Err(ShareConsumerEntryBuildError::NameTooLong);
    }
    if rack.is_some_and(str::is_empty) {
        return Err(ShareConsumerEntryBuildError::EmptyRack);
    }
    if rack.is_some_and(|rack| rack.len() > SHARE_NAME_BYTE_LIMIT) {
        return Err(ShareConsumerEntryBuildError::NameTooLong);
    }
    if topics.is_empty() {
        return Err(ShareConsumerEntryBuildError::EmptySubscription);
    }
    if topics.len() > SHARE_TOPIC_CAPACITY {
        return Err(ShareConsumerEntryBuildError::TopicCapacity);
    }
    for (index, topic) in topics.iter().enumerate() {
        if topic.is_empty() {
            return Err(ShareConsumerEntryBuildError::EmptyTopic);
        }
        if topic.len() > SHARE_NAME_BYTE_LIMIT {
            return Err(ShareConsumerEntryBuildError::NameTooLong);
        }
        if topics[..index].iter().any(|candidate| candidate == topic) {
            return Err(ShareConsumerEntryBuildError::DuplicateTopic);
        }
    }
    Ok(())
}

fn member_spelling() -> Result<Arc<str>, ()> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut source = [0u8; 16];
    SystemRandom::new().fill(&mut source).map_err(|_error| ())?;
    source[6] = (source[6] & 0x0f) | 0x40;
    source[8] = (source[8] & 0x3f) | 0x80;
    let mut spelling = String::new();
    spelling.try_reserve_exact(36).map_err(|_error| ())?;
    for (index, byte) in source.into_iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            spelling.push('-');
        }
        spelling.push(char::from(HEX[usize::from(byte >> 4)]));
        spelling.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(Arc::from(spelling))
}
