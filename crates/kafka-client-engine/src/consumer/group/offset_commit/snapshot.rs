//! Pre-core catalog snapshot and generated request materialization.

use std::sync::Arc;

use kafka_client_core::GroupCheckpoint;

use crate::protocol::consumer::{
    ClassicGroupCommitSession, GroupOffsetCommitTopicName, PreparedGroupOffsetCommitRequest,
};

use super::{
    super::session_catalog::GroupSessionCatalog,
    host::{GroupOffsetCommitHost, GroupOffsetCommitHostError},
};

pub(super) struct PreparedSnapshot {
    pub(super) session: ClassicGroupCommitSession,
    pub(super) topic_names: Vec<GroupOffsetCommitTopicName>,
    pub(super) request: PreparedGroupOffsetCommitRequest,
}

impl GroupOffsetCommitHost {
    pub(super) fn snapshot(
        catalog: &GroupSessionCatalog,
        checkpoint: &GroupCheckpoint,
        mut topic_names: Vec<GroupOffsetCommitTopicName>,
    ) -> Result<PreparedSnapshot, GroupOffsetCommitHostError> {
        let assignment = catalog
            .live_assignment()
            .ok_or(GroupOffsetCommitHostError::Preparation)?;
        let session = ClassicGroupCommitSession::new(
            catalog.group_id(),
            Arc::clone(catalog.group()),
            catalog
                .current_member_id()
                .ok_or(GroupOffsetCommitHostError::Preparation)?,
            Arc::clone(
                catalog
                    .current_member()
                    .ok_or(GroupOffsetCommitHostError::Preparation)?,
            ),
            assignment.assignment_generation(),
            i64::from(
                catalog
                    .classic_generation()
                    .ok_or(GroupOffsetCommitHostError::Preparation)?,
            ),
        )
        .with_group_instance_id(catalog.group_instance_id().cloned());
        let mut last_topic = None;
        for entry in checkpoint.entries() {
            if last_topic == Some(entry.topic_id()) {
                continue;
            }
            topic_names.push(GroupOffsetCommitTopicName::new(
                entry.topic_id(),
                Arc::clone(catalog.topic_name(entry.topic_id())?),
            ));
            last_topic = Some(entry.topic_id());
        }
        let request = PreparedGroupOffsetCommitRequest::try_new(&session, checkpoint, &topic_names)
            .map_err(|_error| GroupOffsetCommitHostError::Preparation)?;
        Ok(PreparedSnapshot {
            session,
            topic_names,
            request,
        })
    }
}
