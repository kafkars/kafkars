//! Closed generated version window for API-key 31 `DeleteAcls`.

/// Oldest `DeleteAcls` version retained by `kafka-wire`.
pub(crate) const DELETE_ACLS_MIN_VERSION: i16 = 1;

/// Newest `DeleteAcls` version retained by `kafka-wire`.
pub(crate) const DELETE_ACLS_MAX_VERSION: i16 = 3;

pub(super) const fn supports_delete_acls_version(version: i16) -> bool {
    version >= DELETE_ACLS_MIN_VERSION && version <= DELETE_ACLS_MAX_VERSION
}
