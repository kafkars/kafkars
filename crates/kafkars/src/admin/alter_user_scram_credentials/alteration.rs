//! Stable caller-ordered SCRAM credential mutation vocabulary.

use std::fmt;

use zeroize::Zeroize;

use super::super::ScramMechanism;

type UpsertParts = (u32, Vec<u8>, Option<Vec<u8>>);
type AlterationParts = (String, ScramMechanism, Option<UpsertParts>);

/// One deletion or upsert of a user's SCRAM credential.
///
/// Construction is inert. User, mechanism, iteration, password, salt, and
/// duplicate-key validation occurs only when the surrounding builder is
/// submitted. Secret bytes are uniquely owned, never cloned, redacted from
/// diagnostics, and zeroized if this value is dropped before submission.
pub enum UserScramCredentialAlteration {
    /// Deletes one user's credential for one mechanism.
    Delete {
        /// Kafka user spelling.
        user: String,
        /// SCRAM mechanism to delete.
        mechanism: ScramMechanism,
    },
    /// Inserts or replaces one user's credential for one mechanism.
    Upsert {
        /// Kafka user spelling.
        user: String,
        /// SCRAM mechanism to upsert.
        mechanism: ScramMechanism,
        /// Requested positive iteration count.
        iterations: u32,
        /// Caller-owned plaintext password bytes.
        password: Vec<u8>,
        /// Optional caller-owned explicit salt bytes.
        salt: Option<Vec<u8>>,
    },
}

impl UserScramCredentialAlteration {
    /// Creates one inert credential deletion.
    pub fn delete(user: impl Into<String>, mechanism: ScramMechanism) -> Self {
        Self::Delete {
            user: user.into(),
            mechanism,
        }
    }

    /// Creates one inert credential upsert using a client-generated salt.
    pub fn upsert(
        user: impl Into<String>,
        mechanism: ScramMechanism,
        iterations: u32,
        password: impl Into<Vec<u8>>,
    ) -> Self {
        Self::Upsert {
            user: user.into(),
            mechanism,
            iterations,
            password: password.into(),
            salt: None,
        }
    }

    /// Creates one inert credential upsert with an explicit caller-owned salt.
    pub fn upsert_with_salt(
        user: impl Into<String>,
        mechanism: ScramMechanism,
        iterations: u32,
        password: impl Into<Vec<u8>>,
        salt: impl Into<Vec<u8>>,
    ) -> Self {
        Self::Upsert {
            user: user.into(),
            mechanism,
            iterations,
            password: password.into(),
            salt: Some(salt.into()),
        }
    }

    /// Returns the affected Kafka user.
    pub fn user(&self) -> &str {
        match self {
            Self::Delete { user, .. } | Self::Upsert { user, .. } => user,
        }
    }

    /// Returns the affected SCRAM mechanism.
    pub const fn mechanism(&self) -> ScramMechanism {
        match self {
            Self::Delete { mechanism, .. } | Self::Upsert { mechanism, .. } => *mechanism,
        }
    }

    /// Returns the requested iteration count for an upsert.
    pub const fn iterations(&self) -> Option<u32> {
        match self {
            Self::Delete { .. } => None,
            Self::Upsert { iterations, .. } => Some(*iterations),
        }
    }

    /// Returns the plaintext password length without exposing its bytes.
    pub fn password_len(&self) -> Option<usize> {
        match self {
            Self::Delete { .. } => None,
            Self::Upsert { password, .. } => Some(password.len()),
        }
    }

    /// Returns the explicit salt length, or `None` when the client should generate it.
    pub fn salt_len(&self) -> Option<usize> {
        match self {
            Self::Delete { .. } => None,
            Self::Upsert { salt, .. } => salt.as_ref().map(Vec::len),
        }
    }

    pub(crate) fn into_parts(mut self) -> AlterationParts {
        match &mut self {
            Self::Delete { user, mechanism } => (std::mem::take(user), *mechanism, None),
            Self::Upsert {
                user,
                mechanism,
                iterations,
                password,
                salt,
            } => (
                std::mem::take(user),
                *mechanism,
                Some((*iterations, std::mem::take(password), std::mem::take(salt))),
            ),
        }
    }
}

impl Drop for UserScramCredentialAlteration {
    fn drop(&mut self) {
        self.zeroize_secrets();
    }
}

impl UserScramCredentialAlteration {
    fn zeroize_secrets(&mut self) {
        if let Self::Upsert { password, salt, .. } = self {
            password.zeroize();
            if let Some(salt) = salt {
                salt.zeroize();
            }
        }
    }

    #[cfg(test)]
    pub(super) fn zeroize_secrets_for_test(&mut self) {
        self.zeroize_secrets();
    }
}

impl fmt::Debug for UserScramCredentialAlteration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Delete { user, mechanism } => formatter
                .debug_struct("Delete")
                .field("user", user)
                .field("mechanism", mechanism)
                .finish(),
            Self::Upsert {
                user,
                mechanism,
                iterations,
                password,
                salt,
            } => formatter
                .debug_struct("Upsert")
                .field("user", user)
                .field("mechanism", mechanism)
                .field("iterations", iterations)
                .field("password_len", &password.len())
                .field("salt_len", &salt.as_ref().map(Vec::len))
                .finish(),
        }
    }
}
