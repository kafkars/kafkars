//! Deterministic policy for one destructive Admin `AlterUserScramCredentials` batch.

mod change;
mod machine;
mod model;
mod outcome;
mod transition;

pub use change::{AlterUserScramCredentialChange, AlterUserScramCredentialChangeKind};
pub use machine::{
    AlterUserScramCredentialsEffect, AlterUserScramCredentialsInput,
    AlterUserScramCredentialsMachine, AlterUserScramCredentialsMachineError,
    AlterUserScramCredentialsState, AlterUserScramCredentialsTransition,
};
pub use model::{
    ALTER_USER_SCRAM_CREDENTIALS_MAX_CHANGES, ALTER_USER_SCRAM_CREDENTIALS_MAX_ITERATIONS,
    ALTER_USER_SCRAM_CREDENTIALS_MAX_USER_NAME_BYTES, ALTER_USER_SCRAM_CREDENTIALS_MAX_USERS,
    ALTER_USER_SCRAM_CREDENTIALS_MIN_ITERATIONS, ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
    ALTER_USER_SCRAM_CREDENTIALS_SHA_512, AlterUserScramCredentialsPlan,
    AlterUserScramCredentialsPlanError,
};
pub use outcome::{
    ALTER_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES, AlterUserScramCredentialBrokerError,
    AlterUserScramCredentialOutcome, AlterUserScramCredentialResult,
    AlterUserScramCredentialsBatch, AlterUserScramCredentialsFailure,
    AlterUserScramCredentialsFailureKind, AlterUserScramCredentialsTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
