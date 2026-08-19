//! Forbidden construction and mutation of classic-group candidate authorities.

use crate::authority_owner::{CandidateMember, ClassicGroupCycleCandidate};

fn intrude() {
    let mut member = CandidateMember {
        joined_slot: 1,
        normalized_member_id: 1,
        ordering_rank: 1,
        kafka_member_spelling: 1,
        subscribed_topic_ids: 1,
        candidate_generation: 1,
        candidate_owned_partitions: 1,
    };
    member.ordering_rank = 2;
    let mut candidate = ClassicGroupCycleCandidate {
        membership_cycle: 1,
        local_catalog_member_id: 1,
        local_kafka_member: 1,
        local_joined_slot: 1,
        ranked_members: 1,
        foreign_topic_bindings: 1,
        member_cursor_after_install: 1,
        topic_cursor_after_install: 1,
        retained_topic_bytes_after_install: 1,
        base_member_cursor: 1,
        base_topic_cursor: 1,
        base_topic_count: 1,
        base_topic_name_bytes: 1,
        local_topic_ids: 1,
    };
    candidate.member_cursor_after_install = 2;
    candidate.foreign_topic_bindings = 2;
}
