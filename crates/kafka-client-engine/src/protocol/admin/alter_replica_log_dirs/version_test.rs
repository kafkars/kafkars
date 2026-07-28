//! Generated API-key 34 name-based version policy.

use super::{
    ALTER_REPLICA_LOG_DIRS_MAX_VERSION, ALTER_REPLICA_LOG_DIRS_MIN_VERSION,
    version::supports_alter_replica_log_dirs_version,
};

#[test]
fn generated_name_based_window_is_exactly_v1_v2() {
    assert_eq!(ALTER_REPLICA_LOG_DIRS_MIN_VERSION, 1);
    assert_eq!(ALTER_REPLICA_LOG_DIRS_MAX_VERSION, 2);
    assert!(supports_alter_replica_log_dirs_version(1));
    assert!(supports_alter_replica_log_dirs_version(2));
    for version in [i16::MIN, 0, 3, i16::MAX] {
        assert!(!supports_alter_replica_log_dirs_version(version));
    }
}
