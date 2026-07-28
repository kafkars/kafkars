//! Closed generated version window for name-based replica log-directory changes.

/// Oldest API-key 34 version retained by generated `kafka-wire`.
pub(crate) const ALTER_REPLICA_LOG_DIRS_MIN_VERSION: i16 = 1;

/// Newest API-key 34 version, differing only by flexible encoding.
pub(crate) const ALTER_REPLICA_LOG_DIRS_MAX_VERSION: i16 = 2;

pub(super) const fn supports_alter_replica_log_dirs_version(version: i16) -> bool {
    version >= ALTER_REPLICA_LOG_DIRS_MIN_VERSION && version <= ALTER_REPLICA_LOG_DIRS_MAX_VERSION
}
