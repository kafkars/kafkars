//! Declarative facade for public Admin `DeleteRecords` values and operation.

mod builder;
mod operation;
mod query;
mod result;
mod value;

pub use builder::DeleteRecordsBuilder;
pub use operation::DeleteRecords;
pub use query::DeleteRecordsTarget;
pub use result::DeleteRecordsResult;
pub use value::DeleteRecordsResultInfo;

#[cfg(test)]
mod query_test;
#[cfg(test)]
mod value_test;
