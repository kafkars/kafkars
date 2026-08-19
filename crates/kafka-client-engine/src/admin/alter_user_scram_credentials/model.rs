//! Engine-owned, secret-redacted intent for Admin `AlterUserScramCredentials`.

use core::fmt;

use kafka_client_core::{
    AlterUserScramCredentialChange as CoreChange, AlterUserScramCredentialsPlan as CorePlan,
    AlterUserScramCredentialsPlanError as CorePlanError,
};
use zeroize::Zeroize;

pub(crate) enum AlterUserScramCredentialsPlanFailure {
    Invalid,
    RetainedBytes,
}

/// One uniquely owned deletion or password-based SCRAM credential upsertion.
///
/// Construction is inert. Validation and password derivation occur only after
/// the enclosing request crosses the capture-first submission boundary.
pub enum AlterUserScramCredential {
    /// Deletes one user's credential for one SCRAM mechanism.
    Delete {
        /// Kafka user spelling.
        user: String,
        /// Kafka's exact SCRAM mechanism code.
        mechanism: i8,
    },
    /// Inserts or replaces one user's credential for one SCRAM mechanism.
    Upsert {
        /// Kafka user spelling.
        user: String,
        /// Kafka's exact SCRAM mechanism code.
        mechanism: i8,
        /// Requested PBKDF2 iteration count.
        iterations: u32,
        /// Uniquely owned plaintext password bytes.
        password: Vec<u8>,
        /// Optional uniquely owned explicit salt.
        salt: Option<Vec<u8>>,
    },
}

impl AlterUserScramCredential {
    /// Creates inert deletion intent.
    pub fn delete(user: String, mechanism: i8) -> Self {
        Self::Delete { user, mechanism }
    }

    /// Creates inert upsertion intent using a client-generated salt.
    pub fn upsert(user: String, mechanism: i8, iterations: u32, password: Vec<u8>) -> Self {
        Self::Upsert {
            user,
            mechanism,
            iterations,
            password,
            salt: None,
        }
    }

    /// Creates inert upsertion intent with an explicit caller-owned salt.
    pub fn upsert_with_salt(
        user: String,
        mechanism: i8,
        iterations: u32,
        password: Vec<u8>,
        salt: Vec<u8>,
    ) -> Self {
        Self::Upsert {
            user,
            mechanism,
            iterations,
            password,
            salt: Some(salt),
        }
    }

    /// Returns the affected Kafka user.
    pub fn user(&self) -> &str {
        match self {
            Self::Delete { user, .. } | Self::Upsert { user, .. } => user,
        }
    }

    /// Returns Kafka's exact requested SCRAM mechanism code.
    pub const fn mechanism(&self) -> i8 {
        match self {
            Self::Delete { mechanism, .. } | Self::Upsert { mechanism, .. } => *mechanism,
        }
    }

    /// Returns the requested iteration count for an upsertion.
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

    /// Returns the explicit salt length, or `None` for generated salt.
    pub fn salt_len(&self) -> Option<usize> {
        match self {
            Self::Delete { .. } => None,
            Self::Upsert { salt, .. } => salt.as_ref().map(Vec::len),
        }
    }

    pub(crate) fn to_core_change(&self) -> CoreChange {
        match self {
            Self::Delete { user, mechanism } => {
                CoreChange::deletion(canonical_string(user), *mechanism)
            }
            Self::Upsert {
                user,
                mechanism,
                iterations,
                ..
            } => CoreChange::upsertion(canonical_string(user), *mechanism, *iterations),
        }
    }

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

impl Drop for AlterUserScramCredential {
    fn drop(&mut self) {
        self.zeroize_secrets();
    }
}

impl fmt::Debug for AlterUserScramCredential {
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

/// One uniquely owned, caller-ordered SCRAM credential alteration request.
pub struct AlterUserScramCredentialsRequest {
    alterations: Vec<AlterUserScramCredential>,
}

impl AlterUserScramCredentialsRequest {
    /// Creates inert alteration intent. Validation remains deferred.
    pub const fn new(alterations: Vec<AlterUserScramCredential>) -> Self {
        Self { alterations }
    }

    /// Returns caller-ordered alteration intent without exposing secret bytes.
    pub fn alterations(&self) -> &[AlterUserScramCredential] {
        &self.alterations
    }

    pub(crate) fn plan(&self) -> Result<CorePlan, AlterUserScramCredentialsPlanFailure> {
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(self.alterations.len())
            .map_err(|_| AlterUserScramCredentialsPlanFailure::RetainedBytes)?;
        changes.extend(
            self.alterations
                .iter()
                .map(AlterUserScramCredential::to_core_change),
        );
        CorePlan::new(changes)
            .map_err(|_error: CorePlanError| AlterUserScramCredentialsPlanFailure::Invalid)
    }
}

impl fmt::Debug for AlterUserScramCredentialsRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterUserScramCredentialsRequest")
            .field("alterations", &self.alterations)
            .finish()
    }
}

fn canonical_string(source: &str) -> String {
    source.to_owned().into_boxed_str().into_string()
}
