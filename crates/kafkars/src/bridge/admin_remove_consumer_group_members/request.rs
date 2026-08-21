//! Inert public static-member removal translated at the engine boundary.

use kafka_client_engine::{
    ConsumerGroupMemberRemoval as EngineMember, RemoveConsumerGroupMembersRequest as EngineRequest,
};

use crate::ConsumerGroupMemberRemoval;

/// Linear request retained by the public builder before submission.
pub(crate) struct RemoveConsumerGroupMembersAdminRequest {
    group_id: String,
    members: Vec<ConsumerGroupMemberRemoval>,
    reason: Option<String>,
}

impl RemoveConsumerGroupMembersAdminRequest {
    pub(crate) const fn new(group_id: String, members: Vec<ConsumerGroupMemberRemoval>) -> Self {
        Self {
            group_id,
            members,
            reason: None,
        }
    }

    pub(crate) fn set_reason(&mut self, reason: String) {
        self.reason = Some(reason);
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            self.group_id,
            self.members
                .into_iter()
                .map(|member| EngineMember::new(member.into_inner()))
                .collect(),
            self.reason,
        )
    }
}

impl std::fmt::Debug for RemoveConsumerGroupMembersAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoveConsumerGroupMembersAdminRequest")
            .field("group_id", &self.group_id)
            .field("members", &self.members)
            .field("reason", &self.reason)
            .finish()
    }
}
