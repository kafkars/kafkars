//! Closed name-based version window for generated `DescribeLogDirs`.

/// Oldest generated API-key 35 version retained by `kafka-wire`.
pub(crate) const DESCRIBE_LOG_DIRS_MIN_VERSION: i16 = 1;

/// Newest generated API-key 35 version, including cordoned-directory state.
pub(crate) const DESCRIBE_LOG_DIRS_MAX_VERSION: i16 = 5;

pub(super) const fn supports_describe_log_dirs_version(version: i16) -> bool {
    version >= DESCRIBE_LOG_DIRS_MIN_VERSION && version <= DESCRIBE_LOG_DIRS_MAX_VERSION
}
