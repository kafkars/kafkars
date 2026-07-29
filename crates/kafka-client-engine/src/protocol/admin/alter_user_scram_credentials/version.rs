//! Exact Kafka version contract for SCRAM credential alteration.

pub(crate) const ALTER_USER_SCRAM_CREDENTIALS_MIN_VERSION: i16 = 0;
pub(crate) const ALTER_USER_SCRAM_CREDENTIALS_MAX_VERSION: i16 = 0;

pub(super) const fn supports_alter_user_scram_credentials_version(version: i16) -> bool {
    version >= ALTER_USER_SCRAM_CREDENTIALS_MIN_VERSION
        && version <= ALTER_USER_SCRAM_CREDENTIALS_MAX_VERSION
}
