//! Exact checked-in expectations for hosted classic rejoin execution.

pub(super) const GROUP_ROOT: &str = "crates/kafka-client-engine/src/consumer/group";
const REJOIN: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_rejoin.rs";
const DUE: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_rejoin_due.rs";
const FAULT: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_rejoin_fault.rs";
const REJECTION_FAULT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_rejection_fault.rs";
const REJECTION_INSTALL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_rejection_install.rs";
const HEARTBEAT_REJECTION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_rejection.rs";
const HEARTBEAT_INTERPRET: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_interpret.rs";
const JOIN_INTERPRET: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_join_interpret.rs";
const SYNC_REJECTION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_sync_rejection.rs";
const MEMBERSHIP: &str = "crates/kafka-client-engine/src/consumer/group/registry_membership.rs";

pub(super) const MIRRORS: &[(&str, &str)] = &[
    (
        REJOIN,
        "crates/kafka-client-engine/src/consumer/group/classic_group_rejoin_test.rs",
    ),
    (
        DUE,
        "crates/kafka-client-engine/src/consumer/group/classic_group_rejoin_due_test.rs",
    ),
    (
        FAULT,
        "crates/kafka-client-engine/src/consumer/group/classic_group_rejoin_fault_test.rs",
    ),
    (
        REJECTION_FAULT,
        "crates/kafka-client-engine/src/consumer/group/classic_group_rejection_fault_test.rs",
    ),
    (
        REJECTION_INSTALL,
        "crates/kafka-client-engine/src/consumer/group/classic_group_rejection_install_test.rs",
    ),
    (
        HEARTBEAT_REJECTION,
        "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_rejection_test.rs",
    ),
    (
        SYNC_REJECTION,
        "crates/kafka-client-engine/src/consumer/group/classic_group_sync_rejection_test.rs",
    ),
];

pub(super) const AUTHORITIES: &[(&str, &str, &[&str], &[&str])] = &[
    (
        "ClassicGroupRejoinExecution",
        REJOIN,
        &["rejoin_execution_state"],
        &[REJOIN],
    ),
    (
        "PendingClassicRejoinJoin",
        FAULT,
        &[
            "pending_rejoin_group_id",
            "pending_rejoin_cycle",
            "pending_rejoin_protocol",
            "pending_rejoin_timing",
            "pending_rejoin_deadline",
        ],
        &[FAULT],
    ),
    (
        "ClassicRejoinPostCore",
        FAULT,
        &[
            "post_core_rejoin_join",
            "post_core_rejoin_other",
            "post_core_rejoin_failure",
        ],
        &[FAULT],
    ),
    (
        "ClassicRejectionPostCore",
        REJECTION_FAULT,
        &["post_core_rejection_effects", "post_core_rejection_failure"],
        &[REJECTION_FAULT],
    ),
];

pub(super) const LINEAR: &[(&str, &str)] = &[
    ("ClassicGroupRejoinState", REJOIN),
    ("ClassicGroupRejoinExecution", REJOIN),
    ("PreparedClassicRejoinInstall", REJOIN),
    ("PendingClassicRejoinJoin", FAULT),
    ("ClassicRejoinPostCore", FAULT),
    ("ClassicRejectionPostCore", REJECTION_FAULT),
    ("ClassicSyncRejectionFailure", SYNC_REJECTION),
];

pub(super) const MUTATION: (&str, &str, &[&str]) = (
    "ClassicGroupRejoinExecution",
    "rejoin_execution_state",
    &[REJOIN],
);

pub(super) const METHODS: &[(&str, &[&str])] = &[
    ("prepare_one_classic_rejoin", &[MEMBERSHIP]),
    (
        "prepare_rejoin_install",
        &[REJECTION_INSTALL, HEARTBEAT_REJECTION],
    ),
    ("clear_rejoin_exact", &[DUE, MEMBERSHIP]),
    ("stage_rejoin_join", &[DUE]),
];

pub(super) const CALLS: &[(&str, &[&str])] = &[
    (
        "exact_broker_error",
        &[JOIN_INTERPRET, SYNC_REJECTION, HEARTBEAT_INTERPRET],
    ),
    ("install_stage_rejection", &[JOIN_INTERPRET, SYNC_REJECTION]),
    ("install_heartbeat_rejection", &[HEARTBEAT_INTERPRET]),
];

pub(super) const REJOIN_FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::clock",
    "crate::completion",
    "crate::driver",
    "crate::host",
    "crate::producer",
    "crate::protocol",
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
    "Instant",
    "Mutex",
    "RwLock",
    "Future",
    "async",
    "Callback",
    "Metadata",
    "Transport",
    "Retry",
];

pub(super) const DUE_FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::completion",
    "crate::driver",
    "crate::host",
    "crate::producer",
    "crate::protocol",
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
    "Instant",
    "Mutex",
    "RwLock",
    "Future",
    "async",
    "Callback",
    "Metadata",
    "Transport",
    "Retry",
    "DeadlineCapture",
    "capture_deadline_after",
    "Duration",
    "OperationDeadline::from_boundary_parts",
];

pub(super) const FAULT_FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::completion",
    "crate::driver",
    "crate::host",
    "crate::producer",
    "crate::protocol",
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
    "Instant",
    "Mutex",
    "RwLock",
    "Future",
    "async",
    "Callback",
    "Metadata",
    "Transport",
    "Retry",
    "DeadlineCapture",
    "capture_deadline_after",
    "Duration",
    "OperationDeadline",
];

pub(super) const CAPABILITIES: &[(&str, &[&str])] = &[
    (REJOIN, REJOIN_FORBIDDEN),
    (DUE, DUE_FORBIDDEN),
    (FAULT, FAULT_FORBIDDEN),
    (REJECTION_FAULT, FAULT_FORBIDDEN),
    (REJECTION_INSTALL, REJECTION_INSTALL_FORBIDDEN),
    (HEARTBEAT_REJECTION, FAULT_FORBIDDEN),
    (SYNC_REJECTION, REJECTION_INSTALL_FORBIDDEN),
];

pub(super) const REJECTION_INSTALL_FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::clock",
    "crate::completion",
    "crate::driver",
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
    "Instant",
    "Mutex",
    "RwLock",
    "Future",
    "async",
    "Callback",
    "Metadata",
    "Transport",
    "Retry",
    "DeadlineCapture",
    "capture_deadline_after",
    "Duration",
    "OperationDeadline",
];

pub(super) const FIXTURE_FORBIDDEN: &[&str] = &[
    "kafka_driver",
    "tokio",
    "std::thread",
    "DeadlineCapture",
    "Duration",
];
