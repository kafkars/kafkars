//! Validated wire-free intent for one caller-ordered SCRAM credential alteration.

use core::fmt;
use std::collections::BTreeSet;

use super::{AlterUserScramCredentialChange, AlterUserScramCredentialChangeKind};

/// Kafka's SCRAM-SHA-256 mechanism code.
pub const ALTER_USER_SCRAM_CREDENTIALS_SHA_256: i8 = 1;
/// Kafka's SCRAM-SHA-512 mechanism code.
pub const ALTER_USER_SCRAM_CREDENTIALS_SHA_512: i8 = 2;
/// Lowest iteration count admitted for an upsertion.
pub const ALTER_USER_SCRAM_CREDENTIALS_MIN_ITERATIONS: u32 = 4096;
/// Highest iteration count admitted for an upsertion.
pub const ALTER_USER_SCRAM_CREDENTIALS_MAX_ITERATIONS: u32 = 16_384;
/// Maximum distinct affected users retained by one operation.
pub const ALTER_USER_SCRAM_CREDENTIALS_MAX_USERS: usize = 1024;
/// Maximum changes retained by one operation.
pub const ALTER_USER_SCRAM_CREDENTIALS_MAX_CHANGES: usize = 1024;
/// Maximum UTF-8 bytes retained for one user name.
pub const ALTER_USER_SCRAM_CREDENTIALS_MAX_USER_NAME_BYTES: usize = i16::MAX as usize;

/// Validated intent for one destructive SCRAM credential RPC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterUserScramCredentialsPlan {
    changes: Vec<AlterUserScramCredentialChange>,
    affected_users: Vec<String>,
}

impl AlterUserScramCredentialsPlan {
    /// Validates bounds and identities while retaining exact caller order.
    pub fn new(
        changes: Vec<AlterUserScramCredentialChange>,
    ) -> Result<Self, AlterUserScramCredentialsPlanError> {
        if changes.is_empty() {
            return Err(AlterUserScramCredentialsPlanError::EmptyBatch);
        }
        if changes.len() > ALTER_USER_SCRAM_CREDENTIALS_MAX_CHANGES {
            return Err(AlterUserScramCredentialsPlanError::TooManyChanges);
        }
        let mut identities = BTreeSet::new();
        let mut users = BTreeSet::new();
        let mut affected_users =
            Vec::with_capacity(changes.len().min(ALTER_USER_SCRAM_CREDENTIALS_MAX_USERS));
        for change in &changes {
            validate_change(change)?;
            if !identities.insert((change.user(), change.mechanism())) {
                return Err(AlterUserScramCredentialsPlanError::DuplicateCredential);
            }
            if users.insert(change.user()) {
                if affected_users.len() == ALTER_USER_SCRAM_CREDENTIALS_MAX_USERS {
                    return Err(AlterUserScramCredentialsPlanError::TooManyUsers);
                }
                affected_users.push(change.user().to_owned());
            }
        }
        Ok(Self {
            changes,
            affected_users,
        })
    }

    /// Returns changes in exact caller order.
    pub fn changes(&self) -> &[AlterUserScramCredentialChange] {
        &self.changes
    }

    /// Returns distinct affected users in first-occurrence order.
    pub fn affected_users(&self) -> &[String] {
        &self.affected_users
    }
}

fn validate_change(
    change: &AlterUserScramCredentialChange,
) -> Result<(), AlterUserScramCredentialsPlanError> {
    if change.user().is_empty() {
        return Err(AlterUserScramCredentialsPlanError::EmptyUserName);
    }
    if change.user().len() > ALTER_USER_SCRAM_CREDENTIALS_MAX_USER_NAME_BYTES {
        return Err(AlterUserScramCredentialsPlanError::UserNameTooLong);
    }
    if !matches!(
        change.mechanism(),
        ALTER_USER_SCRAM_CREDENTIALS_SHA_256 | ALTER_USER_SCRAM_CREDENTIALS_SHA_512
    ) {
        return Err(AlterUserScramCredentialsPlanError::UnknownMechanism);
    }
    if let AlterUserScramCredentialChangeKind::Upsertion { iterations } = change.kind()
        && !(ALTER_USER_SCRAM_CREDENTIALS_MIN_ITERATIONS
            ..=ALTER_USER_SCRAM_CREDENTIALS_MAX_ITERATIONS)
            .contains(&iterations)
    {
        return Err(AlterUserScramCredentialsPlanError::IterationsOutOfRange);
    }
    Ok(())
}

/// Invalid deterministic SCRAM credential alteration intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialsPlanError {
    /// Kafka cannot execute an empty change batch.
    EmptyBatch,
    /// One operation cannot retain more than 1024 changes.
    TooManyChanges,
    /// One operation cannot affect more than 1024 distinct users.
    TooManyUsers,
    /// User names must not be empty.
    EmptyUserName,
    /// User names must fit the client's bounded Kafka string domain.
    UserNameTooLong,
    /// Only SCRAM-SHA-256 and SCRAM-SHA-512 may be altered.
    UnknownMechanism,
    /// Upsertion iterations must be in the inclusive 4096 through 16384 domain.
    IterationsOutOfRange,
    /// One operation cannot repeat a user/mechanism identity.
    DuplicateCredential,
}

impl fmt::Display for AlterUserScramCredentialsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid AlterUserScramCredentials plan: {self:?}"
        )
    }
}

impl std::error::Error for AlterUserScramCredentialsPlanError {}
