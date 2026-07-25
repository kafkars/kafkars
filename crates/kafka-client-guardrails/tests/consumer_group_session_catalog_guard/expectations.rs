//! Exact policy expected for classic-group engine identity.

pub(super) const CATALOG: &str = "crates/kafka-client-engine/src/consumer/group/session_catalog.rs";
pub(super) const CATALOG_ASSIGNMENT: &str =
    "crates/kafka-client-engine/src/consumer/group/session_catalog_assignment.rs";
pub(super) const OWNER: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_owner.rs";
pub(super) const CANDIDATE: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_candidate.rs";
pub(super) const EFFECT_ASSIGNMENT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_assignment.rs";
pub(super) const CAPABILITY_PATHS: &[&str] = &[
    CATALOG,
    CATALOG_ASSIGNMENT,
    OWNER,
    CANDIDATE,
    "crates/kafka-client-engine/src/consumer/group/classic_group_candidate_prepare.rs",
    "crates/kafka-client-engine/src/consumer/group/classic_group_topics.rs",
    EFFECT_ASSIGNMENT,
];
pub(super) const LINEAR: &[(&str, &str)] = &[
    ("GroupSessionCatalog", CATALOG),
    ("ClassicGroupOwner", OWNER),
    ("ClassicGroupCycleCandidate", CANDIDATE),
    ("CandidateMember", CANDIDATE),
    ("PreparedClassicGroupInstall", EFFECT_ASSIGNMENT),
    ("PreparedClassicGroupRevoke", EFFECT_ASSIGNMENT),
];
pub(super) const CATALOG_FIELDS: &[(&str, &[&str])] = &[
    ("next_member_id", &[CATALOG, CATALOG_ASSIGNMENT]),
    ("next_topic_id", &[CATALOG, CATALOG_ASSIGNMENT]),
    ("retained_topic_name_bytes", &[CATALOG, CATALOG_ASSIGNMENT]),
    ("topics_by_name", &[CATALOG, CATALOG_ASSIGNMENT]),
    ("topics_by_id", &[CATALOG, CATALOG_ASSIGNMENT]),
    ("current", &[CATALOG_ASSIGNMENT]),
];
pub(super) const OWNER_FIELDS: &[(&str, &[&str])] = &[("pending", &[OWNER, EFFECT_ASSIGNMENT])];
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
