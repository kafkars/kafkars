//! Linear public SCRAM secrets translated only at engine submission.

use crate::admin::UserScramCredentialAlteration;

use super::engine::{EngineAlteration, Request as EngineRequest};

/// SCRAM alterations retained by the inert public builder.
pub(crate) struct AlterUserScramCredentialsAdminRequest {
    alterations: Vec<UserScramCredentialAlteration>,
}

impl AlterUserScramCredentialsAdminRequest {
    pub(crate) const fn new(alterations: Vec<UserScramCredentialAlteration>) -> Self {
        Self { alterations }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            self.alterations
                .into_iter()
                .map(translate_alteration)
                .collect(),
        )
    }
}

impl std::fmt::Debug for AlterUserScramCredentialsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterUserScramCredentialsAdminRequest")
            .field("alterations", &self.alterations)
            .finish()
    }
}

fn translate_alteration(alteration: UserScramCredentialAlteration) -> EngineAlteration {
    match alteration.into_parts() {
        (user, mechanism, None) => EngineAlteration::delete(user, mechanism.code()),
        (user, mechanism, Some((iterations, password, None))) => {
            EngineAlteration::upsert(user, mechanism.code(), iterations, password)
        }
        (user, mechanism, Some((iterations, password, Some(salt)))) => {
            EngineAlteration::upsert_with_salt(user, mechanism.code(), iterations, password, salt)
        }
    }
}
