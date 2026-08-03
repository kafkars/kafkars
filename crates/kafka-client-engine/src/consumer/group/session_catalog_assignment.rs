//! Narrow mutation seam for committed classic-group assignment facts.

use std::sync::Arc;

use kafka_client_core::{ClassicGeneration, LiveGroupAssignment};

use super::{
    classic_group_candidate::ClassicGroupCycleCandidate,
    session_catalog::{CurrentGroupSession, GroupSessionCatalog},
};

/// Prevalidated broker-generation advance while the prior assignment remains live.
#[must_use = "a prepared classic reconciliation epoch must be committed"]
pub(super) struct PreparedClassicReconciliationEpoch<'a> {
    catalog: &'a mut GroupSessionCatalog,
    classic_generation: ClassicGeneration,
}

impl GroupSessionCatalog {
    pub(super) fn prepare_classic_reconciliation_epoch<'a>(
        &'a mut self,
        candidate: &ClassicGroupCycleCandidate,
        previous: &LiveGroupAssignment,
        previous_generation: ClassicGeneration,
        classic_generation: ClassicGeneration,
    ) -> Option<PreparedClassicReconciliationEpoch<'a>> {
        let current = self.current.as_ref()?;
        if !candidate.matches_catalog_base(self)
            || &current.assignment != previous
            || current.classic_generation != previous_generation.get()
            || current.member_id != candidate.local_member_id()
            || current.member.as_ref() != candidate.local_member().as_ref()
        {
            return None;
        }
        Some(PreparedClassicReconciliationEpoch {
            catalog: self,
            classic_generation,
        })
    }

    pub(super) fn commit_classic_group_install(
        &mut self,
        candidate: ClassicGroupCycleCandidate,
        assignment: LiveGroupAssignment,
        classic_generation: ClassicGeneration,
    ) {
        debug_assert!(self.consumer_current.is_none());
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

    pub(super) fn commit_classic_group_reconciliation_loss(&mut self) {
        self.current = None;
    }
}

impl PreparedClassicReconciliationEpoch<'_> {
    pub(super) fn commit(self) {
        let current = self
            .catalog
            .current
            .as_mut()
            .unwrap_or_else(|| unreachable!("prepared reconciliation retains current session"));
        current.classic_generation = self.classic_generation.get();
    }
}
