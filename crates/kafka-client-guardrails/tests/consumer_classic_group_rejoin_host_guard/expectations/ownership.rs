//! Exact test mirrors and linear ownership expectations for classic rejoin.

pub(crate) const GROUP_ROOT: &str = "crates/kafka-client-engine/src/consumer/group";
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
const HEARTBEAT_REJECTION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_rejection.rs";
const HEARTBEAT_REJECTION_INSTALL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_rejection_install.rs";
const SYNC_REJECTION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_sync_rejection.rs";
const MEMBERSHIP_OBSERVATION: &str =
    "crates/kafka-client-engine/src/consumer/group/registry_membership_observation.rs";

pub(crate) const MIRRORS: &[(&str, &str)] = &[
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
        REDISCOVERY,
        "crates/kafka-client-engine/src/consumer/group/classic_group_rediscovery_test.rs",
    ),
    (
        REDISCOVERY_EXECUTION,
        "crates/kafka-client-engine/src/consumer/group/classic_group_rediscovery_execution_test.rs",
    ),
    (
        REDISCOVERY_RECOVERY,
        "crates/kafka-client-engine/src/consumer/group/classic_group_rediscovery_recovery_test.rs",
    ),
    (
        REDISCOVERY_TRANSFER,
        "crates/kafka-client-engine/src/consumer/group/classic_group_rediscovery_transfer_test.rs",
    ),
    (
        HEARTBEAT_REJECTION,
        "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_rejection_test.rs",
    ),
    (
        HEARTBEAT_REJECTION_INSTALL,
        "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_rejection_install_test.rs",
    ),
    (
        SYNC_REJECTION,
        "crates/kafka-client-engine/src/consumer/group/classic_group_sync_rejection_test.rs",
    ),
    (
        MEMBERSHIP_OBSERVATION,
        "crates/kafka-client-engine/src/consumer/group/registry_membership_observation_test.rs",
    ),
];

pub(crate) const AUTHORITIES: &[(&str, &str, &[&str], &[&str])] = &[
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
    (
        "ClassicCoordinatorRediscovery",
        REDISCOVERY,
        &["rediscovery_state"],
        &[REDISCOVERY],
    ),
];

pub(crate) const LINEAR: &[(&str, &str)] = &[
    ("ClassicGroupRejoinState", REJOIN),
    ("ClassicGroupRejoinExecution", REJOIN),
    ("PreparedClassicRejoinInstall", REJOIN),
    ("PendingClassicRejoinJoin", FAULT),
    ("ClassicRejoinPostCore", FAULT),
    ("ClassicRejectionPostCore", REJECTION_FAULT),
    ("ClassicSyncRejectionFailure", SYNC_REJECTION),
    ("ClassicCoordinatorRediscoveryState", REDISCOVERY),
    ("ClassicCoordinatorRediscovery", REDISCOVERY),
    ("PreparedClassicCoordinatorRediscovery", REDISCOVERY),
];

pub(crate) const MUTATIONS: &[(&str, &str, &[&str])] = &[
    (
        "ClassicGroupRejoinExecution",
        "rejoin_execution_state",
        &[REJOIN],
    ),
    (
        "ClassicCoordinatorRediscovery",
        "rediscovery_state",
        &[REDISCOVERY],
    ),
];
