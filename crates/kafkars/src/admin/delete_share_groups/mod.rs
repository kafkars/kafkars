//! Declarative facade for share-group deletion over the common group protocol.

mod builder;
mod operation;
mod result;

pub use builder::DeleteShareGroupsBuilder;
pub use operation::DeleteShareGroups;
pub use result::DeleteShareGroupsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
