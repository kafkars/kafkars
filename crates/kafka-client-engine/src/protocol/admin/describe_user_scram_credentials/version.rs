//! Closed generated version window for API-key 50.

/// Oldest generated `DescribeUserScramCredentials` version.
pub(crate) const DESCRIBE_USER_SCRAM_CREDENTIALS_MIN_VERSION: i16 = 0;

/// Newest generated `DescribeUserScramCredentials` version.
pub(crate) const DESCRIBE_USER_SCRAM_CREDENTIALS_MAX_VERSION: i16 = 0;

pub(super) const fn supports_describe_user_scram_credentials_version(version: i16) -> bool {
    version >= DESCRIBE_USER_SCRAM_CREDENTIALS_MIN_VERSION
        && version <= DESCRIBE_USER_SCRAM_CREDENTIALS_MAX_VERSION
}
