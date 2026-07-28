//! Exact policy expected for classic-group engine identity.

pub(super) const CATALOG: &str = "crates/kafka-client-engine/src/consumer/group/session_catalog.rs";
pub(super) const CATALOG_STATIC: &str =
    "crates/kafka-client-engine/src/consumer/group/session_catalog/static_membership.rs";
pub(super) const CATALOG_ASSIGNMENT: &str =
    "crates/kafka-client-engine/src/consumer/group/session_catalog_assignment.rs";
pub(super) const OWNER: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_owner.rs";
pub(super) const FOLLOWER_OWNER: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_owner_follower.rs";
pub(super) const LEADER_OWNER: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_owner_leader.rs";
pub(super) const CANDIDATE: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_candidate.rs";
pub(super) const EFFECT_ASSIGNMENT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_assignment.rs";
pub(super) const ASSIGNMENT_DECODE: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_assignment_decode.rs";
pub(super) const ASSIGNMENT_DECODE_TEST: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_assignment_decode_test.rs";
pub(super) const ASSIGNMENT_DECODE_CALL: &str = "decode_classic_group_assignment";
pub(super) const SYNC_INSTALL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_sync_install.rs";
pub(super) const CAPABILITY_PATHS: &[&str] = &[
    CATALOG,
    CATALOG_STATIC,
    CATALOG_ASSIGNMENT,
    OWNER,
    CANDIDATE,
    "crates/kafka-client-engine/src/consumer/group/classic_group_candidate_prepare.rs",
    "crates/kafka-client-engine/src/consumer/group/classic_group_topics.rs",
    EFFECT_ASSIGNMENT,
];
pub(super) const LINEAR: &[(&str, &str)] = &[
    ("GroupSessionCatalog", CATALOG),
    ("RequiredJoinMember", CATALOG_STATIC),
    ("ClassicGroupOwner", OWNER),
    ("ClassicGroupCycleCandidate", CANDIDATE),
    ("CandidateMember", CANDIDATE),
    ("PreparedClassicGroupInstall", EFFECT_ASSIGNMENT),
    ("PreparedClassicGroupRevoke", EFFECT_ASSIGNMENT),
    ("ClassicGroupAssignmentDecodeFailure", ASSIGNMENT_DECODE),
];
pub(super) const CATALOG_FIELDS: &[(&str, &[&str])] = &[
    (
        "next_member_id",
        &[CATALOG, CATALOG_STATIC, CATALOG_ASSIGNMENT],
    ),
    ("next_topic_id", &[CATALOG, CATALOG_ASSIGNMENT]),
    ("retained_topic_name_bytes", &[CATALOG, CATALOG_ASSIGNMENT]),
    ("topics_by_name", &[CATALOG, CATALOG_ASSIGNMENT]),
    ("topics_by_id", &[CATALOG, CATALOG_ASSIGNMENT]),
    ("current", &[CATALOG_ASSIGNMENT]),
    (
        "required_join_member",
        &[CATALOG, CATALOG_STATIC, CATALOG_ASSIGNMENT],
    ),
];
pub(super) const OWNER_FIELDS: &[(&str, &[&str])] = &[(
    "pending",
    &[OWNER, FOLLOWER_OWNER, LEADER_OWNER, EFFECT_ASSIGNMENT],
)];
pub(super) const METHODS: &[(&str, &str)] = &[
    ("commit_classic_group_install", EFFECT_ASSIGNMENT),
    ("commit_classic_group_revoke", EFFECT_ASSIGNMENT),
    (
        "from_prepared_member",
        "crates/kafka-client-engine/src/consumer/group/classic_group_candidate_prepare.rs",
    ),
    (
        "try_from_prepared_cycle",
        "crates/kafka-client-engine/src/consumer/group/classic_group_candidate_prepare.rs",
    ),
    ("into_catalog_install", CATALOG_ASSIGNMENT),
];
pub(super) const AUTHORITIES: &[(&str, &[&str])] = &[
    (
        "CandidateMember",
        &[
            "joined_slot",
            "normalized_member_id",
            "ordering_rank",
            "kafka_member_spelling",
            "subscribed_topic_ids",
        ],
    ),
    (
        "ClassicGroupCycleCandidate",
        &[
            "membership_cycle",
            "local_catalog_member_id",
            "local_kafka_member",
            "local_joined_slot",
            "ranked_members",
            "foreign_topic_bindings",
            "member_cursor_after_install",
            "topic_cursor_after_install",
            "retained_topic_bytes_after_install",
            "base_member_cursor",
            "base_topic_cursor",
            "base_topic_count",
            "base_topic_name_bytes",
            "local_topic_ids",
        ],
    ),
];
pub(super) const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::clock",
    "crate::completion",
    "crate::driver",
    "crate::producer",
    "crate::protocol",
    "crate::transaction",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "std::future",
    "std::net",
    "std::thread",
    "std::time",
    "Condvar",
    "Instant::now",
    "Mutex",
    "RwLock",
    "Future",
    "async",
    "Callback",
    "Metadata",
    "Transport",
    "Retry",
];
pub(super) const DECODE_FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::clock",
    "crate::completion",
    "crate::driver",
    "crate::producer",
    "crate::transaction",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "std::future",
    "std::net",
    "std::thread",
    "std::time",
    "Condvar",
    "Instant::now",
    "Mutex",
    "RwLock",
    "Future",
    "async",
    "Callback",
    "Metadata",
    "Transport",
    "Retry",
];
