//! Public entry point for bounded static consumer-group member removal.

use super::Admin;
use crate::{
    admin::{ConsumerGroupMemberRemoval, RemoveConsumerGroupMembersBuilder},
    bridge::admin_remove_consumer_group_members::RemoveConsumerGroupMembersAdminRequest,
};

impl Admin {
    /// Builds an inert caller-ordered removal of selected static group members.
    ///
    /// No timeout starts and no destructive operation is admitted until
    /// [`RemoveConsumerGroupMembersBuilder::submit`] is called.
    pub fn remove_consumer_group_members<I>(
        &self,
        group_id: impl Into<String>,
        members: I,
    ) -> RemoveConsumerGroupMembersBuilder
    where
        I: IntoIterator<Item = ConsumerGroupMemberRemoval>,
    {
        RemoveConsumerGroupMembersBuilder::new(
            self.engine.clone(),
            RemoveConsumerGroupMembersAdminRequest::new(
                group_id.into(),
                members.into_iter().collect(),
            ),
            self.engine.default_timeout(),
        )
    }
}
