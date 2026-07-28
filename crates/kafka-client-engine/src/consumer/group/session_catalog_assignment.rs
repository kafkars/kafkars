//! Narrow mutation seam for committed classic-group assignment facts.

use std::sync::Arc;

use kafka_client_core::{ClassicGeneration, LiveGroupAssignment};

use super::{
    classic_group_candidate::ClassicGroupCycleCandidate,
    session_catalog::{CurrentGroupSession, GroupSessionCatalog},
};

impl GroupSessionCatalog {
    pub(super) fn commit_classic_group_install(
        &mut self,
        candidate: ClassicGroupCycleCandidate,
        assignment: LiveGroupAssignment,
        classic_generation: ClassicGeneration,
    ) {
        let (staged_topics, next_member_id, next_topic_id, retained_topic_name_bytes, member) =
            candidate.into_catalog_install();
        for (name, topic_id) in staged_topics {
            self.topics_by_name.insert(Arc::clone(&name), topic_id);
            self.topics_by_id.insert(topic_id, name);
        }
        self.next_member_id = next_member_id;
        self.next_topic_id = next_topic_id;
        self.retained_topic_name_bytes = retained_topic_name_bytes;
        self.required_join_member = None;
        self.current = Some(CurrentGroupSession {
            member_id: assignment.member_id(),
            member,
            classic_generation: classic_generation.get(),
            assignment,
        });
    }

    pub(super) fn commit_classic_group_revoke(
        &mut self,
        _assignment: LiveGroupAssignment,
        _classic_generation: ClassicGeneration,
    ) {
        self.current = None;
    }
}
