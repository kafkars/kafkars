//! Exact method, call, and capability expectations for classic rejoin.

const REJOIN: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_rejoin.rs";
const DUE: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_rejoin_due.rs";
const FAULT: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_rejoin_fault.rs";
const REJECTION_FAULT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_rejection_fault.rs";
const REJECTION_INSTALL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_rejection_install.rs";
const REDISCOVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_rediscovery.rs";
const REDISCOVERY_EXECUTION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_rediscovery_execution.rs";
const REDISCOVERY_RECOVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_rediscovery_recovery.rs";
const REDISCOVERY_TRANSFER: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_rediscovery_transfer.rs";
const LEAVE_TURN: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_leave/turn.rs";
const HEARTBEAT_REJECTION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_rejection.rs";
const HEARTBEAT_REJECTION_INSTALL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_rejection_install.rs";
const HEARTBEAT_INTERPRET: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_interpret.rs";
const JOIN_INTERPRET: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_join_interpret.rs";
const SYNC_REJECTION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_sync_rejection.rs";
const MEMBERSHIP: &str = "crates/kafka-client-engine/src/consumer/group/registry_membership.rs";
const MEMBERSHIP_LOCAL: &str =
    "crates/kafka-client-engine/src/consumer/group/registry_membership_local.rs";
const MEMBERSHIP_OBSERVATION: &str =
    "crates/kafka-client-engine/src/consumer/group/registry_membership_observation.rs";

pub(crate) const METHODS: &[(&str, &[&str])] = &[
    ("prepare_one_classic_rejoin", &[MEMBERSHIP]),
    (
        "prepare_rejoin_install",
        &[REJECTION_INSTALL, HEARTBEAT_REJECTION_INSTALL],
    ),
    ("clear_rejoin_exact", &[DUE, MEMBERSHIP_LOCAL]),
    (
        "stage_rejoin_join",
        &[
            DUE,
            "crates/kafka-client-engine/src/consumer/group/classic_group_reconciliation_turn.rs",
        ],
    ),
    (
        "prepare_rediscovery_install",
        &[REJECTION_INSTALL, HEARTBEAT_REJECTION_INSTALL],
    ),
    (
        "confirm_rediscovery_transfer",
        &[REDISCOVERY_TRANSFER, LEAVE_TURN],
    ),
    ("permit_rejoin", &[REDISCOVERY_EXECUTION]),
    (
        "clear_rediscovery_after_driver_shutdown",
        &[REDISCOVERY_RECOVERY],
    ),
];

pub(crate) const CALLS: &[(&str, &[&str])] = &[
    (
        "exact_broker_error",
        &[JOIN_INTERPRET, SYNC_REJECTION, HEARTBEAT_INTERPRET],
    ),
    ("install_stage_rejection", &[JOIN_INTERPRET, SYNC_REJECTION]),
    ("install_heartbeat_rejection", &[HEARTBEAT_INTERPRET]),
];

pub(crate) const CAPABILITIES: &[(&str, &[&str])] = &[
    (REJOIN, REJOIN_FORBIDDEN),
    (DUE, DUE_FORBIDDEN),
    (FAULT, FAULT_FORBIDDEN),
    (REJECTION_FAULT, FAULT_FORBIDDEN),
    (REJECTION_INSTALL, REJECTION_INSTALL_FORBIDDEN),
    (REDISCOVERY, REJOIN_FORBIDDEN),
    (REDISCOVERY_EXECUTION, DRIVER_NEUTRAL_FORBIDDEN),
    (REDISCOVERY_RECOVERY, DRIVER_NEUTRAL_FORBIDDEN),
    (REDISCOVERY_TRANSFER, DRIVER_NEUTRAL_FORBIDDEN),
    (MEMBERSHIP_OBSERVATION, DRIVER_NEUTRAL_FORBIDDEN),
    (HEARTBEAT_REJECTION, FAULT_FORBIDDEN),
    (HEARTBEAT_REJECTION_INSTALL, FAULT_FORBIDDEN),
    (SYNC_REJECTION, REJECTION_INSTALL_FORBIDDEN),
];

const REJOIN_FORBIDDEN: &[&str] = &[
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

const DUE_FORBIDDEN: &[&str] = &[
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

const FAULT_FORBIDDEN: &[&str] = &[
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

const REJECTION_INSTALL_FORBIDDEN: &[&str] = &[
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

const DRIVER_NEUTRAL_FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::clock",
    "crate::completion",
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

pub(crate) const FIXTURE_FORBIDDEN: &[&str] = &[
    "kafka_driver",
    "tokio",
    "std::thread",
    "DeadlineCapture",
    "Duration",
];
