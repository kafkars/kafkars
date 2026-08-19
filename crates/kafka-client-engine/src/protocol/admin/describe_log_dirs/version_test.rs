//! Name-based `DescribeLogDirs` generated-version policy.

use super::{
    DESCRIBE_LOG_DIRS_MAX_VERSION, DESCRIBE_LOG_DIRS_MIN_VERSION,
    version::supports_describe_log_dirs_version,
};

#[test]
fn generated_name_selection_has_an_exact_v1_to_v5_window() {
    assert_eq!(DESCRIBE_LOG_DIRS_MIN_VERSION, 1);
    assert_eq!(DESCRIBE_LOG_DIRS_MAX_VERSION, 5);
    for version in 1..=5 {
        assert!(supports_describe_log_dirs_version(version));
    }
    for version in [i16::MIN, 0, 6, i16::MAX] {
        assert!(!supports_describe_log_dirs_version(version));
    }
}
