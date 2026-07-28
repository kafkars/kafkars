//! Deterministic policy for caller-ordered ACL deletion filters.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    DeleteAclsEffect, DeleteAclsInput, DeleteAclsMachine, DeleteAclsMachineError, DeleteAclsRoute,
    DeleteAclsState, DeleteAclsTransition,
};
pub use model::{DeleteAclsFilter, DeleteAclsPlan, DeleteAclsPlanError, MAX_DELETE_ACLS_FILTERS};
pub use outcome::{
    DELETE_ACLS_DIAGNOSTIC_BYTES, DeleteAclBrokerError, DeleteAclFilterResult,
    DeleteAclMatchResult, DeleteAclMatchingBinding, DeleteAclsBatch, DeleteAclsFailure,
    DeleteAclsFailureKind, DeleteAclsTerminal, MAX_DELETE_ACLS_MATCHING_BINDINGS,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
