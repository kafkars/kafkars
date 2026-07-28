//! Exact static policy expected for the bounded group registry.

#[path = "entry_fields.rs"]
mod entry_fields;
#[path = "registry_fields.rs"]
mod registry_fields;

pub(super) use entry_fields::{ENTRY_DECLARED_FIELDS, ENTRY_FIELDS};
pub(super) use registry_fields::{REGISTRY_DECLARED_FIELDS, REGISTRY_FIELDS};

pub(super) const ROOT: &str = "crates/kafka-client-engine/src/consumer/group/";
pub(super) const REGISTRY_PATH: &str = "crates/kafka-client-engine/src/consumer/group/registry.rs";
pub(super) const ENTRY_PATH: &str =
    "crates/kafka-client-engine/src/consumer/group/registry_entry.rs";
pub(super) const HOST_START_METHOD: &str = "start_group_offset_commit_host";
pub(super) const MIRRORS: &[(&str, &str)] = &[
    ("registry_registration.rs", "registry_test.rs"),
    ("registry_entry.rs", "registry_entry_test.rs"),
    ("registry_commit.rs", "registry_commit_test.rs"),
    ("registry_close.rs", "registry_close_test.rs"),
    ("registry_host.rs", "registry_host_test.rs"),
    ("registry_session.rs", "registry_session_test.rs"),
];
pub(super) const FORBIDDEN: &[&str] = &[
    "crate::driver",
    "crate::host",
    "crate::protocol",
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
    "Retry",
    "invalidate",
    "remove",
    "swap_remove",
    "retain",
    "pop",
    "clear",
    "drain",
];
pub(super) const REGISTRY_HOST_FORBIDDEN: &[&str] = &[
    "crate::admin",
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
    "std::thread::spawn",
    "std::time::Instant",
    "std::time::SystemTime",
    "Condvar",
    "Instant::now",
    "Mutex",
    "RwLock",
    "Future",
    "Callback",
    "Metadata",
    "OperationDeadline",
    "Retry",
    "Route",
    "Runtime",
    "StartedEngineHost",
    "TrafficClass",
    "crate::Engine",
    "crate::exports",
    "async",
    "invalidate",
];
