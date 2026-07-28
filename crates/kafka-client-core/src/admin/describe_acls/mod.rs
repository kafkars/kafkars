//! Deterministic policy for one read-only Admin `DescribeAcls` query.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    DescribeAclsEffect, DescribeAclsInput, DescribeAclsMachine, DescribeAclsMachineError,
    DescribeAclsState, DescribeAclsTransition,
};
pub use model::{DescribeAclsFilter, DescribeAclsPlan, DescribeAclsPlanError};
pub use outcome::{
    DESCRIBE_ACLS_DIAGNOSTIC_BYTES, DescribeAclBinding, DescribeAclsBatch, DescribeAclsBrokerError,
    DescribeAclsFailure, DescribeAclsFailureKind, DescribeAclsTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
