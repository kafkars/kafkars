//! Closed generated version window for API-key 48 `DescribeClientQuotas`.

/// Oldest generated `DescribeClientQuotas` version retained by `kafka-wire`.
pub(crate) const DESCRIBE_CLIENT_QUOTAS_MIN_VERSION: i16 = 0;

/// Newest generated `DescribeClientQuotas` version retained by `kafka-wire`.
pub(crate) const DESCRIBE_CLIENT_QUOTAS_MAX_VERSION: i16 = 1;

pub(super) const fn supports_describe_client_quotas_version(version: i16) -> bool {
    version >= DESCRIBE_CLIENT_QUOTAS_MIN_VERSION && version <= DESCRIBE_CLIENT_QUOTAS_MAX_VERSION
}
