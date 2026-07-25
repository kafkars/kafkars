//! Valid private declarations for classic-group candidate authority fixtures.

struct CandidateMember {
    joined_slot: usize,
    normalized_member_id: usize,
    ordering_rank: usize,
    kafka_member_spelling: usize,
    subscribed_topic_ids: usize,
}

struct ClassicGroupCycleCandidate {
    membership_cycle: usize,
    local_catalog_member_id: usize,
    local_kafka_member: usize,
    local_joined_slot: usize,
    ranked_members: usize,
    foreign_topic_bindings: usize,
    member_cursor_after_install: usize,
    topic_cursor_after_install: usize,
    retained_topic_bytes_after_install: usize,
    base_member_cursor: usize,
    base_topic_cursor: usize,
    base_topic_count: usize,
    base_topic_name_bytes: usize,
    local_topic_ids: usize,
}

fn own() -> (CandidateMember, ClassicGroupCycleCandidate) {
    (
        CandidateMember {
            joined_slot: 1,
            normalized_member_id: 1,
            ordering_rank: 1,
            kafka_member_spelling: 1,
            subscribed_topic_ids: 1,
        },
        ClassicGroupCycleCandidate {
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
        },
    )
}
