//! Closed generated version window for API-key 30 `CreateAcls`.

/// Oldest `CreateAcls` version retained by `kafka-wire`.
pub(crate) const CREATE_ACLS_MIN_VERSION: i16 = 1;

/// Newest `CreateAcls` version retained by `kafka-wire`.
pub(crate) const CREATE_ACLS_MAX_VERSION: i16 = 3;

pub(super) const fn supports_create_acls_version(version: i16) -> bool {
    version >= CREATE_ACLS_MIN_VERSION && version <= CREATE_ACLS_MAX_VERSION
}
