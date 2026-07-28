//! Static classic-group identity and same-cycle broker member staging.

use std::sync::Arc;

use kafka_client_core::{GroupId, MemberId, MembershipCycle};

use super::{
    GroupSessionCatalog, GroupSessionCatalogError, MAX_KAFKA_GROUP_STRING_BYTES,
    validate_kafka_string,
};

/// Broker-assigned spelling staged for one same-cycle KIP-394 replacement Join.
pub(in crate::consumer::group) struct RequiredJoinMember {
    pub(in crate::consumer::group) cycle: MembershipCycle,
    pub(in crate::consumer::group) member_id: MemberId,
    pub(in crate::consumer::group) member: Arc<str>,
}

impl GroupSessionCatalog {
    pub(in crate::consumer::group) fn try_new_with_group_instance_id(
        group_id: GroupId,
        group: Arc<str>,
        group_instance_id: Option<Arc<str>>,
        local_topics: &[Arc<str>],
    ) -> Result<Self, GroupSessionCatalogError> {
        if let Some(group_instance_id) = &group_instance_id {
            validate_kafka_string(
                group_instance_id,
                GroupSessionCatalogError::EmptyGroupInstance,
                |actual| GroupSessionCatalogError::GroupInstanceBytes {
                    actual,
                    limit: MAX_KAFKA_GROUP_STRING_BYTES,
                },
            )?;
        }
        let mut catalog = Self::try_new(group_id, group, local_topics)?;
        catalog.group_instance_id = group_instance_id;
        Ok(catalog)
    }

    pub(in crate::consumer::group) fn group_instance_id(&self) -> Option<&Arc<str>> {
        self.group_instance_id.as_ref()
    }

    pub(in crate::consumer::group) fn retained_identity_bytes(&self) -> usize {
        self.group
            .len()
            .saturating_add(self.group_instance_id.as_ref().map_or(0, |id| id.len()))
    }

    pub(in crate::consumer::group) fn prepare_required_join_member(
        &self,
        cycle: MembershipCycle,
        member: Arc<str>,
    ) -> Result<RequiredJoinMember, GroupSessionCatalogError> {
        validate_kafka_string(&member, GroupSessionCatalogError::EmptyMember, |actual| {
            GroupSessionCatalogError::MemberBytes {
                actual,
                limit: MAX_KAFKA_GROUP_STRING_BYTES,
            }
        })?;
        let member_id = self
            .next_member_id
            .ok_or(GroupSessionCatalogError::Allocation)?;
        Ok(RequiredJoinMember {
            cycle,
            member_id,
            member,
        })
    }

    pub(in crate::consumer::group) fn commit_required_join_member(
        &mut self,
        required: RequiredJoinMember,
    ) {
        self.required_join_member = Some(required);
    }

    pub(in crate::consumer::group) fn required_join_member_spelling(
        &self,
        cycle: MembershipCycle,
        member_id: MemberId,
    ) -> Option<&Arc<str>> {
        self.required_join_member
            .as_ref()
            .filter(|required| required.cycle == cycle && required.member_id == member_id)
            .map(|required| &required.member)
    }
}
