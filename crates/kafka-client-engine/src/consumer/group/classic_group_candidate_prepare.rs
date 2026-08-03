//! Atomic preparation of one owned classic-group cycle candidate.

use std::{collections::BTreeMap, sync::Arc};

use kafka_client_core::{ClassicProtocol, MemberId, MemberRank, MembershipCycle};

use super::{
    classic_group_candidate::{
        CandidateMember, ClassicGroupCycleCandidate, ClassicGroupCycleCandidateError,
        JoinedGroupMember, PreparedClassicGroupCycle,
    },
    classic_group_topics::{PreparedCycleTopics, local_ownership_matches},
    session_catalog::{GroupSessionCatalog, MAX_KAFKA_GROUP_STRING_BYTES, validate_kafka_string},
};

const MAX_JOINED_MEMBERS: usize = 64;
const MAX_TOPICS_PER_MEMBER: usize = 64;

impl GroupSessionCatalog {
    pub(super) fn prepare_follower_cycle(
        &self,
        cycle: MembershipCycle,
        local_member: Arc<str>,
    ) -> Result<ClassicGroupCycleCandidate, ClassicGroupCycleCandidateError> {
        validate_member(&local_member)?;
        let (local_member_id, next_member_id) = match self.required_join_member.as_ref() {
            Some(required)
                if required.cycle == cycle && required.member.as_ref() == local_member.as_ref() =>
            {
                (
                    required.member_id,
                    required
                        .member_id
                        .get()
                        .checked_add(1)
                        .and_then(MemberId::try_from_raw),
                )
            }
            Some(required) if required.cycle == cycle => {
                return Err(ClassicGroupCycleCandidateError::RequiredMemberMismatch);
            }
            _ if self
                .current
                .as_ref()
                .is_some_and(|current| current.member.as_ref() == local_member.as_ref()) =>
            {
                (
                    self.current
                        .as_ref()
                        .unwrap_or_else(|| unreachable!("matching current member exists"))
                        .member_id,
                    self.next_member_id,
                )
            }
            _ => allocate_member(self.next_member_id)?,
        };
        ClassicGroupCycleCandidate::try_from_prepared_cycle(
            self,
            cycle,
            PreparedClassicGroupCycle {
                local_member_id,
                local_member,
                local_slot: None,
                members: Vec::new(),
                next_member_id,
                staged_topics: BTreeMap::new(),
                next_topic_id: self.next_topic_id,
                retained_topic_name_bytes: self.retained_topic_name_bytes,
            },
        )
    }

    pub(super) fn prepare_leader_cycle(
        &self,
        cycle: MembershipCycle,
        protocol: ClassicProtocol,
        local_member: Arc<str>,
        mut joined: Vec<JoinedGroupMember>,
    ) -> Result<ClassicGroupCycleCandidate, ClassicGroupCycleCandidateError> {
        validate_member(&local_member)?;
        validate_leader_members(self, protocol, &local_member, &mut joined)?;

        let mut topics = PreparedCycleTopics::new(self);
        let required = self
            .required_join_member
            .as_ref()
            .filter(|required| required.cycle == cycle);
        if required.is_some_and(|required| required.member.as_ref() != local_member.as_ref()) {
            return Err(ClassicGroupCycleCandidateError::RequiredMemberMismatch);
        }
        let mut next_member_id = match required {
            Some(required) => required
                .member_id
                .get()
                .checked_add(1)
                .and_then(MemberId::try_from_raw),
            None => self.next_member_id,
        };
        let mut members = Vec::new();
        members
            .try_reserve_exact(joined.len())
            .map_err(|_error| ClassicGroupCycleCandidateError::Allocation)?;
        let mut local = None;
        for (index, member) in joined.into_iter().enumerate() {
            let member_id = if member.member.as_ref() == local_member.as_ref() {
                if let Some(required) = required {
                    required.member_id
                } else if let Some(current) = self
                    .current
                    .as_ref()
                    .filter(|current| current.member.as_ref() == local_member.as_ref())
                {
                    current.member_id
                } else {
                    let (member_id, next) = allocate_member(next_member_id)?;
                    next_member_id = next;
                    member_id
                }
            } else {
                let (member_id, next) = allocate_member(next_member_id)?;
                next_member_id = next;
                member_id
            };
            let rank = u32::try_from(index + 1)
                .ok()
                .and_then(MemberRank::try_from_raw)
                .ok_or(ClassicGroupCycleCandidateError::RankExhausted)?;
            let translated = topics.translate_subscription(member.topics)?;
            let translated_owned = topics.translate_owned(member.owned_partitions)?;
            if member.member.as_ref() == local_member.as_ref() {
                if translated != self.local_subscription() {
                    return Err(ClassicGroupCycleCandidateError::LocalSubscriptionMismatch);
                }
                local = Some((member_id, member.slot));
            }
            members.push(CandidateMember::from_prepared_member_with_owned(
                member.slot,
                member_id,
                rank,
                member.member,
                translated,
                translated_owned,
                member.generation,
            ));
        }
        let (local_member_id, local_slot) =
            local.ok_or(ClassicGroupCycleCandidateError::LocalMemberMissing)?;
        ClassicGroupCycleCandidate::try_from_prepared_cycle(
            self,
            cycle,
            PreparedClassicGroupCycle {
                local_member_id,
                local_member,
                local_slot: Some(local_slot),
                members,
                next_member_id,
                staged_topics: topics.staged,
                next_topic_id: topics.next_topic_id,
                retained_topic_name_bytes: topics.retained_topic_name_bytes,
            },
        )
    }
}

fn validate_leader_members(
    catalog: &GroupSessionCatalog,
    protocol: ClassicProtocol,
    local_member: &str,
    joined: &mut [JoinedGroupMember],
) -> Result<(), ClassicGroupCycleCandidateError> {
    if joined.is_empty() || joined.len() > MAX_JOINED_MEMBERS {
        return Err(ClassicGroupCycleCandidateError::MemberCapacity {
            actual: joined.len(),
            limit: MAX_JOINED_MEMBERS,
        });
    }
    joined.sort_unstable_by(|left, right| left.member.as_ref().cmp(right.member.as_ref()));
    if joined
        .windows(2)
        .any(|pair| pair[0].member == pair[1].member)
    {
        return Err(ClassicGroupCycleCandidateError::DuplicateMember);
    }
    for (index, member) in joined.iter().enumerate() {
        validate_member(&member.member)?;
        if joined[..index]
            .iter()
            .any(|prior| prior.slot == member.slot)
        {
            return Err(ClassicGroupCycleCandidateError::DuplicateSlot(member.slot));
        }
        if member.topics.len() > MAX_TOPICS_PER_MEMBER {
            return Err(ClassicGroupCycleCandidateError::TopicsPerMember {
                actual: member.topics.len(),
                limit: MAX_TOPICS_PER_MEMBER,
            });
        }
    }
    if protocol == ClassicProtocol::CooperativeSticky {
        let local = joined
            .iter()
            .find(|member| member.member.as_ref() == local_member)
            .ok_or(ClassicGroupCycleCandidateError::LocalMemberMissing)?;
        if !local_ownership_matches(catalog, local) {
            return Err(ClassicGroupCycleCandidateError::LocalOwnershipMismatch);
        }
    }
    Ok(())
}

fn validate_member(member: &str) -> Result<(), ClassicGroupCycleCandidateError> {
    validate_kafka_string(
        member,
        super::session_catalog::GroupSessionCatalogError::EmptyMember,
        |actual| super::session_catalog::GroupSessionCatalogError::MemberBytes {
            actual,
            limit: MAX_KAFKA_GROUP_STRING_BYTES,
        },
    )
    .map_err(|error| match error {
        super::session_catalog::GroupSessionCatalogError::EmptyMember => {
            ClassicGroupCycleCandidateError::EmptyMember
        }
        super::session_catalog::GroupSessionCatalogError::MemberBytes { actual, limit } => {
            ClassicGroupCycleCandidateError::MemberBytes { actual, limit }
        }
        _ => ClassicGroupCycleCandidateError::Catalog(error),
    })
}

fn allocate_member(
    next: Option<MemberId>,
) -> Result<(MemberId, Option<MemberId>), ClassicGroupCycleCandidateError> {
    let member_id = next.ok_or(ClassicGroupCycleCandidateError::MemberIdentityExhausted)?;
    Ok((
        member_id,
        member_id
            .get()
            .checked_add(1)
            .and_then(MemberId::try_from_raw),
    ))
}
