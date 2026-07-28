//! Closed generated version window for API-key 49 `AlterClientQuotas`.

/// Oldest generated `AlterClientQuotas` version retained by `kafka-wire`.
pub(crate) const ALTER_CLIENT_QUOTAS_MIN_VERSION: i16 = 0;

/// Newest generated `AlterClientQuotas` version retained by `kafka-wire`.
pub(crate) const ALTER_CLIENT_QUOTAS_MAX_VERSION: i16 = 1;

pub(super) const fn supports_alter_client_quotas_version(version: i16) -> bool {
    version >= ALTER_CLIENT_QUOTAS_MIN_VERSION && version <= ALTER_CLIENT_QUOTAS_MAX_VERSION
}
