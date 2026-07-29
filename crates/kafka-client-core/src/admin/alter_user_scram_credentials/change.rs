//! Non-secret caller intent for one user and SCRAM mechanism.

/// Semantic change for one SCRAM credential.
///
/// Upsertion retains only the bounded iteration intent. Passwords, salts,
/// salted passwords, keys, and cryptographic work remain outside core.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialChangeKind {
    /// Deletes the selected user/mechanism credential.
    Deletion,
    /// Inserts or replaces the credential with the requested iteration count.
    Upsertion {
        /// PBKDF2 iteration count requested by the caller.
        iterations: u32,
    },
}

/// One caller-ordered non-secret SCRAM credential change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterUserScramCredentialChange {
    user: String,
    mechanism: i8,
    kind: AlterUserScramCredentialChangeKind,
}

impl AlterUserScramCredentialChange {
    /// Creates one deletion for enclosing-plan validation.
    pub const fn deletion(user: String, mechanism: i8) -> Self {
        Self {
            user,
            mechanism,
            kind: AlterUserScramCredentialChangeKind::Deletion,
        }
    }

    /// Creates one upsertion intent containing no credential material.
    pub const fn upsertion(user: String, mechanism: i8, iterations: u32) -> Self {
        Self {
            user,
            mechanism,
            kind: AlterUserScramCredentialChangeKind::Upsertion { iterations },
        }
    }

    /// Returns the affected user.
    pub fn user(&self) -> &str {
        &self.user
    }

    /// Returns Kafka's exact known SCRAM mechanism code.
    pub const fn mechanism(&self) -> i8 {
        self.mechanism
    }

    /// Returns deletion or non-secret upsertion intent.
    pub const fn kind(&self) -> AlterUserScramCredentialChangeKind {
        self.kind
    }

    /// Consumes this change into adapter-owned non-secret parts.
    pub fn into_parts(self) -> (String, i8, AlterUserScramCredentialChangeKind) {
        (self.user, self.mechanism, self.kind)
    }
}
