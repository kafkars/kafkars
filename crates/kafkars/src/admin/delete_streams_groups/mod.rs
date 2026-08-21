//! Declarative facade for streams-group deletion over the common group protocol.

mod builder;
mod operation;
mod result;

pub use builder::DeleteStreamsGroupsBuilder;
pub use operation::DeleteStreamsGroups;
pub use result::DeleteStreamsGroupsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
