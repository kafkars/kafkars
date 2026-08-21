//! Declarative private bridge for `ShareGroup` offset deletion.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDeleteShareGroupOffsets;
pub(crate) use request::DeleteShareGroupOffsetsAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
