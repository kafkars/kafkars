//! Exact mutation surfaces for private group offset-commit host state.

pub(super) const HOST: &[(&str, &[&str])] = &[
    (
        "operations",
        &[
            "host.rs",
            "preparation.rs",
            "publication.rs",
            "recovery.rs",
            "recovery_replay.rs",
            "settlement.rs",
            "turn.rs",
        ],
    ),
    (
        "retained_bytes",
        &[
            "host.rs",
            "admission.rs",
            "preparation.rs",
            "publication.rs",
        ],
    ),
    ("accepting", &["host.rs"]),
    (
        "fault",
        &[
            "host.rs",
            "preparation.rs",
            "recovery.rs",
            "recovery_replay.rs",
            "rollback.rs",
            "settlement.rs",
            "turn.rs",
        ],
    ),
    ("preparation_fault", &["host.rs", "preparation_failure.rs"]),
    (
        "settlement_fault",
        &["host.rs", "recovery_replay.rs", "settlement.rs"],
    ),
    (
        "shutdown_recovery",
        &["host.rs", "recovery.rs", "recovery_replay.rs"],
    ),
    (
        "effect_fault",
        &[
            "host.rs",
            "preparation_failure.rs",
            "recovery_replay.rs",
            "settlement.rs",
        ],
    ),
    ("recovery_faults", &["host.rs", "recovery.rs"]),
    ("published_bytes", &["host.rs", "publication.rs"]),
    ("reclaim_pending", &["host.rs", "publication.rs"]),
    ("next_operation_id", &["admission.rs"]),
];

pub(super) const OPERATION: &[(&str, &[&str])] =
    &[("attempt", &["host.rs"]), ("terminal", &["host.rs"])];
