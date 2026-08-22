//! Exact capability vocabulary for direct Fetch-executor ownership checks.

pub(super) const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::producer",
    "crate::transaction",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "std::future",
    "std::time",
    "Instant::now",
    "Future",
    "async",
    "Transport",
    "Retry",
    "Metadata",
];

pub(super) const CAPABILITY_ALLOWS: &[(&str, &str)] = &[
    ("admission_test.rs", "std::time"),
    ("admission_test.rs", "Instant::now"),
    ("control_test.rs", "std::time"),
    ("control_test.rs", "Instant::now"),
    ("deadline.rs", "std::time"),
    ("deadline_test.rs", "std::time"),
    ("deadline_test.rs", "Instant::now"),
    ("fault_test.rs", "std::time"),
    ("settlement_test.rs", "std::time"),
    ("settlement_test.rs", "Instant::now"),
    ("broker_close.rs", "std::time"),
    ("executor.rs", "std::time"),
];
