//! Public ACL description builder, result, and named operation.

mod builder;
mod operation;
mod result;

pub use builder::DescribeAclsBuilder;
pub use operation::DescribeAcls;
pub use result::DescribeAclsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
