//! Exact checked-in ownership expectations for hosted classic Heartbeat execution.

pub(super) const ROOT: &str = "crates/kafka-client-engine/src";
pub(super) const GROUP_ROOT: &str = "crates/kafka-client-engine/src/consumer/group";
pub(super) const HEARTBEAT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat.rs";
pub(super) const PREPARE: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_prepare.rs";
pub(super) const SUBMISSION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_submission.rs";
pub(super) const INTERPRET: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_interpret.rs";
pub(super) const SETTLEMENT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_settlement.rs";
pub(super) const RECOVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_recovery.rs";
pub(super) const MEMBERSHIP: &str =
    "crates/kafka-client-engine/src/consumer/group/registry_membership.rs";
pub(super) const MEMBERSHIP_LOCAL: &str =
    "crates/kafka-client-engine/src/consumer/group/registry_membership_local.rs";

pub(super) const LINEAR: &[(&str, &str)] = &[
    ("PreparedClassicHeartbeat", HEARTBEAT),
    ("ClassicHeartbeatDriverOwner", HEARTBEAT),
    ("ClassicHeartbeatAcceptanceFailure", HEARTBEAT),
    ("ClassicHeartbeatSuccessor", HEARTBEAT),
    ("ClassicHeartbeatExecutionState", HEARTBEAT),
    ("ClassicHeartbeatExecution", HEARTBEAT),
    ("PreparedClassicHeartbeatInstall", HEARTBEAT),
    ("ClassicHeartbeatInterpretationFailure", INTERPRET),
];

pub(super) const MIRRORS: &[(&str, &str)] = &[
    (
        HEARTBEAT,
        "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_test.rs",
    ),
    (
        PREPARE,
        "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_prepare_test.rs",
    ),
    (
        SUBMISSION,
        "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_submission_test.rs",
    ),
    (
        INTERPRET,
        "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_interpret_test.rs",
    ),
    (
        SETTLEMENT,
        "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_settlement_test.rs",
    ),
    (
        RECOVERY,
        "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_recovery_test.rs",
    ),
];

pub(super) const METHODS: &[(&str, &[&str])] = &[
    ("prepare_one_classic_heartbeat", &[MEMBERSHIP]),
    ("expire_one_prepared_heartbeat", &[MEMBERSHIP_LOCAL]),
    ("submit_one_classic_heartbeat", &[MEMBERSHIP]),
    ("settle_one_classic_heartbeat", &[MEMBERSHIP]),
    (
        "recover_classic_heartbeats_after_driver_shutdown",
        &[
            "crates/kafka-client-engine/src/consumer/group/classic_group_recovery.rs",
            RECOVERY,
        ],
    ),
    (
        "prepare_install",
        &["crates/kafka-client-engine/src/consumer/group/classic_group_sync_install.rs"],
    ),
];

pub(super) const CALLS: &[(&str, &[&str])] = &[
    ("classic_heartbeat_request_with_instance", &[PREPARE]),
    ("interpret_heartbeat", &[SETTLEMENT]),
    ("normalize_classic_heartbeat_response", &[INTERPRET]),
];

pub(super) const BASE_FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::completion",
    "crate::host",
    "crate::producer",
    "crate::transaction",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "tokio",
    "async_std",
    "smol",
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
    "Route",
    "invalidate",
    "capture_deadline_after",
    "DeadlineCapture",
    "OperationDeadline::from_boundary_parts",
];

pub(super) const CAPABILITIES: &[(&str, &[&str])] = &[
    (HEARTBEAT, &["crate::clock"]),
    (PREPARE, &[]),
    (SUBMISSION, &["crate::clock", "crate::protocol"]),
    (INTERPRET, &["crate::clock"]),
    (SETTLEMENT, &["crate::clock", "crate::protocol"]),
    (RECOVERY, &["crate::clock", "crate::protocol"]),
];
