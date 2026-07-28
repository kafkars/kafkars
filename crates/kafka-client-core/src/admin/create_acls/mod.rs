//! Deterministic policy for caller-ordered ACL creation batches.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    CreateAclsEffect, CreateAclsInput, CreateAclsMachine, CreateAclsMachineError, CreateAclsRoute,
    CreateAclsState, CreateAclsTransition,
};
pub use model::{CreateAclBinding, CreateAclsPlan, CreateAclsPlanError, MAX_CREATE_ACLS_BINDINGS};
pub use outcome::{
    CREATE_ACLS_DIAGNOSTIC_BYTES, CreateAclBrokerError, CreateAclResult, CreateAclsBatch,
    CreateAclsFailure, CreateAclsFailureKind, CreateAclsTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
