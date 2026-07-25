//! Exact checked-in classic-group ownership expectations.

pub(super) const ROOT: &str = "crates/kafka-client-core/src/consumer/classic_group";
pub(super) const MACHINE: &str = "crates/kafka-client-core/src/consumer/classic_group/machine.rs";
pub(super) const TRANSITION: &str =
    "crates/kafka-client-core/src/consumer/classic_group/transition.rs";
pub(super) const TERMINAL_TRANSITION: &str =
    "crates/kafka-client-core/src/consumer/classic_group/terminal_transition.rs";

pub(super) const MIRRORS: &[(&str, &str)] = &[
    ("identity.rs", "identity_test.rs"),
    ("model.rs", "model_test.rs"),
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
];

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
];

pub(super) const MACHINE_FIELDS: &[(&str, &[&str])] = &[
    ("phase", &[TRANSITION, TERMINAL_TRANSITION]),
    ("next_cycle", &[TRANSITION, TERMINAL_TRANSITION]),
    ("active_cycle", &[TRANSITION, TERMINAL_TRANSITION]),
    ("deadline", &[TRANSITION, TERMINAL_TRANSITION]),
    ("pending_member_id", &[TRANSITION]),
    ("pending_generation", &[TRANSITION]),
    ("pending_members", &[TRANSITION]),
    ("pending_local_slot", &[TRANSITION]),
    ("pending_expected_assignment", &[TRANSITION]),
    (
        "next_assignment_generation",
        &[TRANSITION, TERMINAL_TRANSITION],
    ),
    ("live_generation", &[TRANSITION, TERMINAL_TRANSITION]),
    ("live_assignment", &[TRANSITION, TERMINAL_TRANSITION]),
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
