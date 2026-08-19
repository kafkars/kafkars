//! Declarative private bridge for `ShareGroup` offset alteration.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminAlterShareGroupOffsets;
pub(crate) use request::AlterShareGroupOffsetsAdminRequest;

#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
