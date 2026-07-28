//! Deterministic policy for one read-only Admin `DescribeUserScramCredentials` query.

mod machine;
mod model;
mod outcome;
mod transition;
mod value;

pub use machine::{
    DescribeUserScramCredentialsEffect, DescribeUserScramCredentialsInput,
    DescribeUserScramCredentialsMachine, DescribeUserScramCredentialsMachineError,
    DescribeUserScramCredentialsState, DescribeUserScramCredentialsTransition,
};
pub use model::{
    DESCRIBE_USER_SCRAM_CREDENTIALS_MAX_USERS, DescribeUserScramCredentialsPlan,
    DescribeUserScramCredentialsPlanError,
};
pub use outcome::{
    DESCRIBE_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES, DescribeUserScramCredentialsBatch,
    DescribeUserScramCredentialsBrokerError, DescribeUserScramCredentialsFailure,
    DescribeUserScramCredentialsFailureKind, DescribeUserScramCredentialsTerminal,
    DescribeUserScramCredentialsUserOutcome, DescribeUserScramCredentialsUserResult,
};
pub use value::ScramCredentialInfo;

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod transition_test;
