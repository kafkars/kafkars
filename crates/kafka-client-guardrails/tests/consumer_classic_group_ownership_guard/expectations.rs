//! Exact checked-in classic-group ownership expectations.

pub(super) const ROOT: &str = "crates/kafka-client-core/src/consumer/classic_group";
pub(super) const MACHINE: &str = "crates/kafka-client-core/src/consumer/classic_group/machine.rs";
pub(super) const TRANSITION: &str =
    "crates/kafka-client-core/src/consumer/classic_group/transition.rs";
pub(super) const TERMINAL_TRANSITION: &str =
    "crates/kafka-client-core/src/consumer/classic_group/terminal_transition.rs";
pub(super) const REJOIN_TRANSITION: &str =
    "crates/kafka-client-core/src/consumer/classic_group/recovery/rejoin_transition.rs";
pub(super) const MEMBER_ID_REQUIRED: &str =
    "crates/kafka-client-core/src/consumer/classic_group/member_id_required.rs";

pub(super) const MIRRORS: &[(&str, &str)] = &[
    ("identity.rs", "identity_test.rs"),
    ("model.rs", "model_test.rs"),
    ("timing.rs", "timing_test.rs"),
    ("assignment.rs", "assignment_test.rs"),
    ("apply.rs", "apply_test.rs"),
    ("input.rs", "input_test.rs"),
    ("effect.rs", "effect_test.rs"),
    ("error.rs", "error_test.rs"),
    ("machine.rs", "machine_test.rs"),
    ("range_validation.rs", "range_validation_test.rs"),
    ("transition.rs", "transition_test.rs"),
    ("terminal_transition.rs", "terminal_transition_test.rs"),
    ("transition_support.rs", "transition_support_test.rs"),
    ("member_id_required.rs", "member_id_required_test.rs"),
    ("heartbeat.rs", "heartbeat_test.rs"),
    ("heartbeat_state.rs", "heartbeat_state_test.rs"),
    ("heartbeat_transition.rs", "heartbeat_transition_test.rs"),
    ("processing_lease.rs", "processing_lease_test.rs"),
    ("recovery/broker_error.rs", "recovery/broker_error_test.rs"),
    (
        "recovery/error_disposition.rs",
        "recovery/error_disposition_test.rs",
    ),
    ("recovery/rejoin.rs", "recovery/rejoin_test.rs"),
    (
        "recovery/rejection_transition.rs",
        "recovery/rejection_transition_test.rs",
    ),
    (
        "recovery/rejoin_transition.rs",
        "recovery/rejoin_transition_test.rs",
    ),
    (
        "graceful_revocation/machine.rs",
        "graceful_revocation/machine_test.rs",
    ),
    (
        "graceful_revocation/model.rs",
        "graceful_revocation/model_test.rs",
    ),
];

pub(super) const IMMUTABLE_MACHINE_FIELDS: &[&str] = &["group_id", "timing", "rejoin_policy"];
pub(super) const TRAILING_MACHINE_FIELDS: &[&str] = &["heartbeat"];

pub(super) const LINEAR: &[(&str, &str)] = &[
    ("ClassicSubscription", "model.rs"),
    ("ClassicJoinMember", "model.rs"),
    ("ClassicJoinMembers", "model.rs"),
    ("ClassicAssignmentPlan", "assignment.rs"),
    ("ClassicMemberAssignment", "assignment.rs"),
    ("ClassicGroupInput", "input.rs"),
    ("ClassicGroupEffect", "effect.rs"),
    ("ClassicGroupTransition", "effect.rs"),
    ("ClassicGroupMachine", "machine.rs"),
    ("ClassicHeartbeatState", "heartbeat_state.rs"),
    ("ClassicProcessingLease", "processing_lease/machine.rs"),
    (
        "PreparedClassicProcessingLeaseActivation",
        "processing_lease/machine.rs",
    ),
    (
        "PreparedClassicProcessingLeaseRevocation",
        "processing_lease/machine.rs",
    ),
    (
        "ClassicGracefulRevocation",
        "graceful_revocation/machine.rs",
    ),
];

pub(super) const MACHINE_FIELDS: &[(&str, &[&str])] = &[
    (
        "phase",
        &[TRANSITION, TERMINAL_TRANSITION, REJOIN_TRANSITION],
    ),
    (
        "next_cycle",
        &[TRANSITION, TERMINAL_TRANSITION, REJOIN_TRANSITION],
    ),
    (
        "active_cycle",
        &[TRANSITION, TERMINAL_TRANSITION, REJOIN_TRANSITION],
    ),
    (
        "deadline",
        &[TRANSITION, TERMINAL_TRANSITION, REJOIN_TRANSITION],
    ),
    ("pending_member_id", &[MEMBER_ID_REQUIRED, TRANSITION]),
    ("pending_generation", &[MEMBER_ID_REQUIRED, TRANSITION]),
    ("pending_members", &[MEMBER_ID_REQUIRED, TRANSITION]),
    ("pending_local_slot", &[MEMBER_ID_REQUIRED, TRANSITION]),
    (
        "pending_expected_assignment",
        &[MEMBER_ID_REQUIRED, TRANSITION],
    ),
    (
        "pending_heartbeat_liveness",
        &[MEMBER_ID_REQUIRED, TRANSITION],
    ),
    (
        "next_assignment_generation",
        &[TRANSITION, TERMINAL_TRANSITION],
    ),
    ("live_generation", &[TRANSITION, TERMINAL_TRANSITION]),
    ("live_assignment", &[TRANSITION, TERMINAL_TRANSITION]),
    (
        "pending_rejoin",
        &[TRANSITION, TERMINAL_TRANSITION, REJOIN_TRANSITION],
    ),
    ("fatal", &[REJOIN_TRANSITION]),
];

pub(super) const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::exports",
    "crate::producer",
    "crate::public_api",
    "crate::transaction",
    "AssignedConsumerMachine",
    "Callback",
    "Clock",
    "Engine",
    "Future",
    "Generated",
    "HashMap",
    "HashSet",
    "Metadata",
    "OperationDeadline",
    "Retry",
    "Runtime",
    "String",
    "Transport",
    "Wire",
    "async",
    "async_std",
    "bytes",
    "kafka_client",
    "kafka_client_engine",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "smol",
    "std::env",
    "std::fs",
    "std::future",
    "std::io",
    "std::net",
    "std::os",
    "std::process",
    "std::sync",
    "std::thread",
    "std::time",
    "str",
    "tokio",
    "u8",
];
