//! Public ACL deletion builder, nested result values, and named operation.

mod builder;
mod operation;
mod result;

pub use builder::DeleteAclsBuilder;
pub use operation::DeleteAcls;
pub use result::{
    DeleteAclBrokerError, DeleteAclFilterOutcome, DeleteAclFilterResult, DeleteAclMatchOutcome,
    DeleteAclMatchResult, DeleteAclsResult,
};

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
