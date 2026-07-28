//! Closed generated version window for API-key 29 `DescribeAcls`.

/// Oldest `DescribeAcls` version retained by `kafka-wire`.
pub(crate) const DESCRIBE_ACLS_MIN_VERSION: i16 = 1;

/// Newest `DescribeAcls` version retained by `kafka-wire`.
pub(crate) const DESCRIBE_ACLS_MAX_VERSION: i16 = 3;

pub(super) const fn supports_describe_acls_version(version: i16) -> bool {
    version >= DESCRIBE_ACLS_MIN_VERSION && version <= DESCRIBE_ACLS_MAX_VERSION
}
